// 1:1 port of all-text-in-nyc/src/lib/gsv/google-streetview-helper.ts
// + all-text-in-nyc/src/lib/gsv/google-streetview.ts

const FOV_FACTOR = 1.4;
const FOV_MIN = 10;

function degreesToRadians(degrees) {
  return degrees * (Math.PI / 180);
}

function radiansToDegrees(radians) {
  return radians * (180 / Math.PI);
}

function correctOCRCoordinatesForRoll(ocrYaw, ocrPitch, streetViewRoll) {
  // The Google embed API levels the panorama's roll (straightens the horizon)
  // but keeps camera-relative pitch. We must correct for roll so that our
  // coordinates match the roll-leveled view the embed displays.
  const ocrYawRad = degreesToRadians(ocrYaw);
  const ocrPitchRad = degreesToRadians(ocrPitch);
  const rollRad = degreesToRadians(-streetViewRoll); // Negate for inverse (undo roll)

  // Convert to Cartesian
  const x = Math.cos(ocrYawRad) * Math.cos(ocrPitchRad);
  const y = Math.sin(ocrYawRad) * Math.cos(ocrPitchRad);
  const z = Math.sin(ocrPitchRad);

  // Apply inverse roll rotation (around x-axis)
  const x2 = x;
  const y2 = y * Math.cos(rollRad) - z * Math.sin(rollRad);
  const z2 = y * Math.sin(rollRad) + z * Math.cos(rollRad);

  // Convert back to spherical
  const correctedYaw = radiansToDegrees(Math.atan2(y2, x2));
  const correctedPitch = radiansToDegrees(Math.asin(z2));

  return {
    ocrYaw: (correctedYaw + 360) % 360,
    ocrPitch: correctedPitch,
  };
}

function fovToZoomLevel(fov) {
  if (fov <= 0 || fov >= 180) {
    throw new Error('FOV must be between 0 and 180 degrees');
  }

  const zoomLevel = Math.log2(360 / fov);
  return Math.round(zoomLevel * 100) / 100;
}

function getGoogleStreetviewProps(
  panoramaId,
  lat,
  lon,
  ocrYaw,
  ocrPitch,
  streetViewHeading,
  streetViewPitch,
  streetViewRoll,
  ocrWidth,
  ocrHeight,
) {
  // The Google embed API pitch is camera-relative, but it levels the roll
  // (straightens the horizon). Apply roll-only correction so coordinates
  // match the roll-leveled view.
  const { ocrYaw: correctedYaw, ocrPitch: correctedPitch } =
    correctOCRCoordinatesForRoll(ocrYaw, ocrPitch, streetViewRoll);

  const ocrBoundingDegree = Math.ceil(
    Math.max(ocrWidth, ocrHeight) * FOV_FACTOR,
  );

  const fov = Math.max(ocrBoundingDegree, FOV_MIN);
  const zoom = fovToZoomLevel(fov);

  return {
    lat: lat,
    lon: lon,
    panoramaId: panoramaId,
    heading: (((correctedYaw + streetViewHeading) % 360) + 360) % 360,
    pitch: correctedPitch,
    tilt: correctedPitch + 90,
    fov: fov,
    zoom: zoom,
  };
}

function getGoogleStreetviewUrl(gsvProp) {
  const { lat, lon, panoramaId, heading, pitch, tilt, fov, zoom } = gsvProp;

  return `https://www.google.com/maps/@${lat},${lon},3a,${20}y,${heading.toFixed(2)}h,${tilt.toFixed(2)}t/data=!3m6!1e1!3m4!1s${panoramaId}!2e0!7i16384!8i8192?entry=ttu`;
}

function getGoogleStreetviewEmbedUrl(gsvProp, GOOGLE_MAPS_API_KEY) {
  const { lat, lon, panoramaId, heading, pitch, tilt, fov, zoom } = gsvProp;

  const baseURL = 'https://www.google.com/maps/embed/v1/streetview';
  const params = new URLSearchParams();
  params.append('key', GOOGLE_MAPS_API_KEY);
  params.append('location', `${lat},${lon}`);
  params.append('pano', panoramaId);
  params.append('heading', `${heading.toFixed(2)}`);
  params.append('pitch', `${pitch.toFixed(2)}`);
  params.append('fov', `${fov}`);

  return `${baseURL}?${params.toString()}`;
}
