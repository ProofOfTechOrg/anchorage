export default class UnavailableStagingRestClient {
  constructor() {
    throw new Error(
      'Cloudflare REST client is unavailable in notification staging',
    );
  }
}
