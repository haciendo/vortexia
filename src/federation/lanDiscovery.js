// Same-LAN peer discovery via mDNS (Bonjour/Zeroconf) — the "both Macs are
// on the same WiFi" case from docs/federation-poc.md's design discussion.
// No relay, no internet, no rate limits, sub-second latency: each vortexia
// instance advertises itself, and discovers any sibling instance
// advertising the same service type on the local network automatically.
//
// This module only does discovery — it answers "who else is on this LAN
// and how do I reach them," not "how do I talk to them once found." A
// discovered peer's {host, mqttPort} is enough for a direct MQTT
// connection to their broker, bypassing the Relay/mailbox model in
// relay.js entirely (no store-and-forward needed when both sides are
// live on the same network right now). Wiring that direct connection into
// FederationBridge is the next step — see docs/federation-poc.md.
//
// NOTE: same-host discovery (advertise and discover from the same
// process/machine) is used here only to prove the advertise/parse code
// paths work — it is NOT proof that multicast actually crosses two real
// machines on a real Wi-Fi network. Some routers/APs enable client
// isolation (each device is firewalled from every other device on the
// same Wi-Fi) or block multicast entirely, which same-host testing can't
// catch. Treat this as tested-in-isolation, not tested-cross-machine.

const SERVICE_TYPE = 'vortexia';

export class LanDiscovery {
  constructor({ envName, mqttPort, wsPort }) {
    if (!envName || !mqttPort) throw new Error('LanDiscovery requires envName and mqttPort');
    this.envName = envName;
    this.mqttPort = mqttPort;
    this.wsPort = wsPort;
    this._bonjour = null;
    this._service = null;
    this._browser = null;
    /** @type {Map<string, {envName: string, host: string, mqttPort: number, wsPort: number|null}>} */
    this.peers = new Map();
  }

  async _ensure() {
    if (this._bonjour) return;
    const { default: Bonjour } = await import('bonjour-service');
    this._bonjour = new Bonjour();
  }

  /** Announce this environment's gateway on the local network. */
  async advertise() {
    await this._ensure();
    this._service = this._bonjour.publish({
      name: `vortexia-${this.envName}`,
      type: SERVICE_TYPE,
      port: this.mqttPort,
      txt: { envName: this.envName, wsPort: String(this.wsPort ?? '') },
    });
    return this;
  }

  /**
   * Start watching for peers. `onUp`/`onDown` fire for every discovered
   * environment other than this one, whenever the local peer map changes.
   */
  async discover({ onUp, onDown } = {}) {
    await this._ensure();
    this._browser = this._bonjour.find({ type: SERVICE_TYPE });
    this._browser.on('up', (service) => this._handleUp(service, onUp));
    this._browser.on('down', (service) => this._handleDown(service, onDown));
    return this;
  }

  _handleUp(service, cb) {
    const peerEnvName = service.txt?.envName;
    if (!peerEnvName || peerEnvName === this.envName) return; // ignore our own advertisement
    const host = service.referer?.address || service.addresses?.[0] || service.host;
    const wsPort = service.txt?.wsPort ? Number(service.txt.wsPort) : null;
    const peer = { envName: peerEnvName, host, mqttPort: service.port, wsPort };
    this.peers.set(peerEnvName, peer);
    cb?.(peer);
  }

  _handleDown(service, cb) {
    const peerEnvName = service.txt?.envName;
    if (!peerEnvName) return;
    this.peers.delete(peerEnvName);
    cb?.(peerEnvName);
  }

  stop() {
    this._browser?.stop();
    this._bonjour?.unpublishAll();
    this._bonjour?.destroy();
    this._bonjour = null;
    this._browser = null;
    this._service = null;
  }
}
