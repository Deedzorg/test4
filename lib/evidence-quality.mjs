export const LEGACY_SUPERVISOR_RELEASE = 'v1.3-final-assurance';

export function storagePersistenceVerdict(previous, current) {
  const previousIdentity = previous?.identity || null;
  const currentIdentity = current?.identity || null;
  if (!previousIdentity || !currentIdentity) return 'pending';
  return previousIdentity === currentIdentity ? 'pass' : 'fail';
}

export function databasePersistenceVerdict(previous, current) {
  const previousIdentity = previous?.identity || null;
  const currentIdentity = current?.identity || null;
  if (!previousIdentity || !currentIdentity) return 'pending';

  const sameIdentity = previousIdentity === currentIdentity;
  const sameEndpoint = !previous?.endpointFingerprint || !current?.endpointFingerprint ||
    previous.endpointFingerprint === current.endpointFingerprint;
  const rowsPreserved = Number(current?.rows || 0) >= Number(previous?.rows || 0);
  const bootAdvanced = Number(current?.bootCount || 0) > Number(previous?.bootCount || 0);

  return sameIdentity && sameEndpoint && rowsPreserved && bootAdvanced ? 'pass' : 'fail';
}

export function persistenceActual(testKey, current, previous) {
  if (testKey === 'storage-persistence') {
    return {
      ...current,
      previousIdentity: previous?.identity || null
    };
  }
  return {
    ...current,
    previous: previous || null
  };
}

export function decorateActiveStatus(payload, { version, release }) {
  if (!payload || typeof payload !== 'object') return payload;
  payload.version = version;
  payload.release = { ...release };
  payload.evidenceQuality = {
    version,
    releaseId: release.releaseId,
    baseSupervisorVersion: '1.3.2',
    releaseLabelsNormalized: true
  };
  return payload;
}

export function decorateEvidencePayload(payload, context) {
  if (!payload || typeof payload !== 'object') return payload;
  payload.schemaVersion = Math.max(Number(payload.schemaVersion || 0), 4);
  payload.version = context.version;
  payload.release = { ...context.release };
  if (payload.runtimeStatus) decorateActiveStatus(payload.runtimeStatus, context);
  payload.evidenceQuality = {
    version: context.version,
    releaseId: context.release.releaseId,
    baseSupervisorVersion: '1.3.2',
    comparatorRepair: 'raw prior observations are compared across distinct runtime instances',
    operatorEvidence: 'out-of-band platform observations are stored as explicit correlated verification records',
    releaseLabelsNormalized: true
  };
  return payload;
}
