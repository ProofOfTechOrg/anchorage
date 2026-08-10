// Module 2/6 — Content Pipeline: research → THREE parallel section writers
// (.parallel fan-out/fan-in) → approval gate → publish to R2. Capabilities
// shown: parallel step execution with a post-fan-in gate, a real R2ArtifactStore
// write (offline-real via InMemoryArtifactBucket), and idempotent publish keyed
// on a content hash so a replay never double-publishes.

import {
  createConnector,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
} from '@proofoftech/breakwater';
import type { WorkflowModule } from '@proofoftech/flowsafe/host-kit/module';
import { z } from 'zod';
import {
  callConnector,
  type ShowcaseModuleDeps,
  showcaseResumeSchema,
} from '#worker/workflows/shared';

export const PUBLISH_CONNECTOR = 'publish-article';
const WORKFLOW_ID = 'content-pipeline';

// djb2 — a small, dependency-free stable content hash. The idempotency key
// combines it with the runId (see publishContent): within a run a replay of the
// same article dedupes; across DIFFERENT runs (each independently approved) the
// runId keeps the keys distinct, so two runs publishing identical content never
// collapse into one physical write.
function contentHash(text: string): string {
  let hash = 5381;
  for (let i = 0; i < text.length; i += 1) {
    hash = (Math.imul(hash, 33) + text.charCodeAt(i)) | 0;
  }
  return `content-${(hash >>> 0).toString(16)}`;
}

const sectionInput = z.object({
  outline: z.array(z.string()),
  sources: z.array(z.string()),
});
const sectionOutput = z.object({ content: z.string() });

export const contentPipelineModule: WorkflowModule<ShowcaseModuleDeps> = {
  meta: {
    id: WORKFLOW_ID,
    title: 'Content Pipeline',
    description:
      'Research a topic, write intro/body/conclusion in parallel, then publish the assembled article to R2 after review. Shows parallel fan-out/fan-in and idempotent publish.',
    sampleInput: { topic: 'durable workflows' },
  },
  register({ createWorkflow, createStep, audit, deps }) {
    // The gated write: persists the approved article to the R2 artifact store.
    // idempotencyKey => the publishContent step must supply a per-call key
    // (the content hash); a replay with the same content is deduped by the
    // store instead of writing twice.
    const publishArticle = createConnector<
      { workflowId: string; runId: string; name: string; article: string },
      { published: boolean; key: string }
    >({
      id: PUBLISH_CONNECTOR,
      description: 'Publishes the approved article to the R2 artifact store',
      inputSchema: z.object({
        workflowId: z.string(),
        runId: z.string(),
        name: z.string(),
        article: z.string(),
      }),
      outputSchema: z.object({ published: z.boolean(), key: z.string() }),
      permissions: {
        sideEffect: 'write',
        requiresApproval: true,
        idempotencyKey: true,
      },
      policies: {
        audit,
        idempotencyStore: deps.idempotency,
      },
      execute: async ({ workflowId, runId, name, article }) => {
        const record = await deps.artifactStore.put(
          { workflowId, runId, name },
          article,
          { contentType: 'text/markdown' },
        );
        console.log(
          JSON.stringify({
            type: 'article-published',
            key: record.key,
            size: article.length,
          }),
        );
        return { published: true, key: record.key };
      },
    });

    // researchTopic builds a real outline (sections from the topic) + sources.
    const researchTopic = createStep({
      id: 'researchTopic',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: sectionInput,
      execute: async ({ inputData }) => {
        const { topic } = inputData;
        return {
          outline: [
            `Why ${topic} matters`,
            `How ${topic} works`,
            `${topic} in practice`,
          ],
          sources: [
            `https://example.com/${encodeURIComponent(topic)}/primer`,
            `https://example.com/${encodeURIComponent(topic)}/case-study`,
          ],
        };
      },
    });

    // Three parallel writers, each receiving researchTopic's output. .parallel
    // fans them out; the fan-in is keyed by step id (writeIntro/Body/Conclusion).
    const writeIntro = createStep({
      id: 'writeIntro',
      inputSchema: sectionInput,
      outputSchema: sectionOutput,
      execute: async ({ inputData }) => ({
        content: `## Introduction\n\n${inputData.outline[0]}. This piece draws on ${inputData.sources.length} sources.`,
      }),
    });
    const writeBody = createStep({
      id: 'writeBody',
      inputSchema: sectionInput,
      outputSchema: sectionOutput,
      execute: async ({ inputData }) => ({
        content: `## Body\n\n${inputData.outline.slice(1).join('. ')}.`,
      }),
    });
    const writeConclusion = createStep({
      id: 'writeConclusion',
      inputSchema: sectionInput,
      outputSchema: sectionOutput,
      execute: async ({ inputData }) => ({
        content: `## Conclusion\n\nIn summary: ${(inputData.outline[0] ?? '').toLowerCase()}.`,
      }),
    });

    // The gate runs AFTER the parallel fan-in. Its inputData is keyed by the
    // parallel step ids (proven in the Phase-0 spike). It assembles the article
    // and suspends for review, declaring the publish connector's grant.
    const reviewContent = createStep({
      id: 'reviewContent',
      inputSchema: z.object({
        writeIntro: sectionOutput,
        writeBody: sectionOutput,
        writeConclusion: sectionOutput,
      }),
      outputSchema: z.object({
        approved: z.boolean(),
        article: z.string(),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        sectionCount: z.number(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        const article = [
          inputData.writeIntro.content,
          inputData.writeBody.content,
          inputData.writeConclusion.content,
        ].join('\n\n');
        if (!resumeData) {
          return suspend({
            reason: 'human review required before publish',
            connectors: [PUBLISH_CONNECTOR],
            sectionCount: 3,
          });
        }
        return {
          approved: resumeData.approved,
          article,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    const publishContent = createStep({
      id: 'publishContent',
      inputSchema: z.object({
        approved: z.boolean(),
        article: z.string(),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({ published: z.boolean(), key: z.string() }),
      execute: async ({ inputData, requestContext, runId }) => {
        if (!inputData.approved) return { published: false, key: '' };
        // R2 key is workflowId/runId/name; idempotency key is runId + the
        // content hash so a same-run replay dedupes but distinct runs never do.
        return callConnector<
          { workflowId: string; runId: string; name: string; article: string },
          { published: boolean; key: string }
        >(
          publishArticle,
          {
            workflowId: WORKFLOW_ID,
            runId,
            name: 'article.md',
            article: inputData.article,
          },
          requestContext,
          {
            [IDEMPOTENCY_KEY_CONTEXT_KEY]: `${runId}:${contentHash(inputData.article)}`,
          },
        );
      },
    });

    createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ published: z.boolean(), key: z.string() }),
    })
      .then(researchTopic)
      .parallel([writeIntro, writeBody, writeConclusion])
      .then(reviewContent)
      .then(publishContent)
      .commit();
  },
};
