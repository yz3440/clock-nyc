(function () {
  const size = 64;
  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext('2d');

  let link = document.querySelector("link[rel='icon']");
  if (!link) {
    link = document.createElement('link');
    link.rel = 'icon';
    link.type = 'image/png';
    document.head.appendChild(link);
  }

  const gold = '#d4b44c';
  const goldLight = '#f5e6a3';
  const goldBright = '#fff4c2';
  const goldMid = '#c9a84c';
  const goldDark = '#7a5c1f';

  function goldGradLR(x1, x2) {
    const g = ctx.createLinearGradient(x1, 0, x2, 0);
    g.addColorStop(0, goldDark);
    g.addColorStop(0.25, goldMid);
    g.addColorStop(0.45, goldBright);
    g.addColorStop(0.55, goldLight);
    g.addColorStop(0.75, goldMid);
    g.addColorStop(1, goldDark);
    return g;
  }

  function draw() {
    const now = new Date();
    const h = now.getHours() % 12;
    const m = now.getMinutes();
    const s = now.getSeconds(); // used for smooth minute hand
    const cx = size / 2;
    const cy = 30;
    const r = 22;

    ctx.clearRect(0, 0, size, size);

    // === Background — Grand Central teal/green ===
    const bg = ctx.createLinearGradient(0, 0, 0, size);
    bg.addColorStop(0, '#1a3a3a');
    bg.addColorStop(0.5, '#1f4040');
    bg.addColorStop(1, '#0f2828');
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, size, size);

    // === Draw base elements FIRST (behind clock) ===

    // --- Neck below clock to base ---
    const neckTop = cy + r + 1;
    const neckBot = 52;

    ctx.beginPath();
    ctx.moveTo(cx - 5, neckTop);
    ctx.quadraticCurveTo(cx - 4, (neckTop + neckBot) / 2, cx - 9, neckBot);
    ctx.lineTo(cx + 9, neckBot);
    ctx.quadraticCurveTo(cx + 4, (neckTop + neckBot) / 2, cx + 5, neckTop);
    ctx.closePath();
    ctx.fillStyle = goldGradLR(cx - 9, cx + 9);
    ctx.fill();

    // --- Squat pedestal base ---
    const baseTop = 52;
    const baseBot = 59;
    const baseWide = 18;
    const baseTopW = 10;

    ctx.beginPath();
    ctx.moveTo(cx - baseTopW, baseTop);
    ctx.quadraticCurveTo(cx - baseWide - 1, baseTop + 2, cx - baseWide, baseBot - 1);
    ctx.lineTo(cx - baseWide, baseBot);
    ctx.lineTo(cx + baseWide, baseBot);
    ctx.lineTo(cx + baseWide, baseBot - 1);
    ctx.quadraticCurveTo(cx + baseWide + 1, baseTop + 2, cx + baseTopW, baseTop);
    ctx.closePath();
    ctx.fillStyle = goldGradLR(cx - baseWide, cx + baseWide);
    ctx.fill();

    // Fluting lines
    ctx.lineWidth = 0.5;
    for (let i = -3; i <= 3; i++) {
      const topX = cx + i * 2.5;
      const botX = cx + i * 4.8;
      ctx.beginPath();
      ctx.moveTo(topX, baseTop + 1);
      ctx.lineTo(botX, baseBot - 1);
      ctx.strokeStyle = i === 0 ? goldBright : (Math.abs(i) <= 1 ? goldLight : goldDark);
      ctx.stroke();
    }

    // Bottom flat rim
    ctx.beginPath();
    ctx.rect(cx - baseWide, baseBot, baseWide * 2, 2.5);
    ctx.fillStyle = goldGradLR(cx - baseWide, cx + baseWide);
    ctx.fill();

    // === Clock sphere (drawn on top of base) ===

    // --- Gold sphere body — 3D shading ---
    const orbR = r + 4;
    // Base gold fill
    const orbGrad = ctx.createRadialGradient(cx - orbR * 0.3, cy - orbR * 0.3, 0, cx, cy, orbR);
    orbGrad.addColorStop(0, goldBright);
    orbGrad.addColorStop(0.3, goldLight);
    orbGrad.addColorStop(0.65, gold);
    orbGrad.addColorStop(0.85, goldDark);
    orbGrad.addColorStop(1, '#4a3510');
    ctx.beginPath();
    ctx.arc(cx, cy, orbR, 0, Math.PI * 2);
    ctx.fillStyle = orbGrad;
    ctx.fill();

    // Specular highlight — glossy reflection upper-left
    const specGrad = ctx.createRadialGradient(cx - 8, cy - 8, 0, cx - 6, cy - 6, orbR * 0.6);
    specGrad.addColorStop(0, 'rgba(255,255,245,0.7)');
    specGrad.addColorStop(0.5, 'rgba(255,250,220,0.2)');
    specGrad.addColorStop(1, 'rgba(255,250,220,0)');
    ctx.beginPath();
    ctx.arc(cx, cy, orbR, 0, Math.PI * 2);
    ctx.fillStyle = specGrad;
    ctx.fill();

    // Rim light — subtle bright edge on upper arc
    ctx.beginPath();
    ctx.arc(cx, cy, orbR - 0.5, -Math.PI * 0.85, -Math.PI * 0.15);
    ctx.strokeStyle = 'rgba(255,255,240,0.45)';
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // --- Small golden peak at top ---
    const peakBase = cy - orbR;
    ctx.beginPath();
    ctx.moveTo(cx, peakBase - 6);
    ctx.lineTo(cx - 4, peakBase + 2);
    ctx.lineTo(cx + 4, peakBase + 2);
    ctx.closePath();
    ctx.fillStyle = goldGradLR(cx - 4, cx + 4);
    ctx.fill();

    // --- White opal face (inset into sphere) ---
    const faceR = r - 5;
    ctx.beginPath();
    ctx.arc(cx, cy, faceR, 0, Math.PI * 2);
    const faceGrad = ctx.createRadialGradient(cx - 3, cy - 3, 0, cx, cy, faceR);
    faceGrad.addColorStop(0, '#fffff8');
    faceGrad.addColorStop(0.7, '#f5f0e0');
    faceGrad.addColorStop(1, '#e8e0cc');
    ctx.fillStyle = faceGrad;
    ctx.fill();

    // Subtle inner shadow on face edge to look recessed
    const insetShadow = ctx.createRadialGradient(cx, cy, faceR - 3, cx, cy, faceR);
    insetShadow.addColorStop(0, 'rgba(0,0,0,0)');
    insetShadow.addColorStop(1, 'rgba(0,0,0,0.08)');
    ctx.beginPath();
    ctx.arc(cx, cy, faceR, 0, Math.PI * 2);
    ctx.fillStyle = insetShadow;
    ctx.fill();

    // --- Side face slivers (other clock faces peeking out) ---
    // Left sliver
    ctx.beginPath();
    ctx.ellipse(cx - orbR + 1, cy, 7, faceR * 0.7, 0, -Math.PI / 2, Math.PI / 2);
    ctx.fillStyle = 'rgba(255,254,248,0.45)';
    ctx.fill();

    // Right sliver
    ctx.beginPath();
    ctx.ellipse(cx + orbR - 1, cy, 7, faceR * 0.7, 0, Math.PI / 2, -Math.PI / 2);
    ctx.fillStyle = 'rgba(255,254,248,0.35)';
    ctx.fill();

    // --- Hour ticks ---
    for (let i = 0; i < 12; i++) {
      const angle = (i * Math.PI) / 6 - Math.PI / 2;
      const inner = faceR - 3;
      const outer = faceR - 1;
      ctx.beginPath();
      ctx.moveTo(cx + inner * Math.cos(angle), cy + inner * Math.sin(angle));
      ctx.lineTo(cx + outer * Math.cos(angle), cy + outer * Math.sin(angle));
      ctx.lineWidth = 2;
      ctx.strokeStyle = goldDark;
      ctx.stroke();
    }

    // --- Hour hand ---
    const hAngle = ((h + m / 60) * Math.PI) / 6 - Math.PI / 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(cx + (faceR * 0.5) * Math.cos(hAngle), cy + (faceR * 0.5) * Math.sin(hAngle));
    ctx.lineWidth = 5;
    ctx.strokeStyle = '#1a1a1a';
    ctx.lineCap = 'round';
    ctx.stroke();

    // --- Minute hand ---
    const mAngle = ((m + s / 60) * Math.PI) / 30 - Math.PI / 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(cx + (faceR * 0.75) * Math.cos(mAngle), cy + (faceR * 0.75) * Math.sin(mAngle));
    ctx.lineWidth = 3.5;
    ctx.strokeStyle = '#1a1a1a';
    ctx.lineCap = 'round';
    ctx.stroke();

    // --- Center dot ---
    ctx.beginPath();
    ctx.arc(cx, cy, 3, 0, Math.PI * 2);
    ctx.fillStyle = goldMid;
    ctx.fill();

    link.href = canvas.toDataURL('image/png');
  }

  draw();
  setInterval(draw, 1000);
})();
