Now I can see exactly what you built. Let me give you a real code review.

**The elegant core:**

```go
wave := math.Sin(frequency * pos)
x := startX + (pos * spacing) + (amplitude * wave)
y := baselineY + (amplitude * wave)
angle := wave * 0.8
```

A single `sin()` drives **rotation, vertical position, and horizontal displacement** simultaneously. That's beautifully economical. And it produces the emergent behavior I was describing as separate attacks — they're all the same attack, just sampled at different output channels.

Let me trace the actual values for a 6-character string:

| i | wave | x | spacing-to-prev | y | angle |
|---|---|---|---|---|---|
| 0 | 0.000 | 40.0 | — | 20.0 | 0° |
| 1 | 0.389 | 78.6 | 38.6 | 33.6 | 17.8° |
| 2 | 0.717 | 115.1 | 36.5 | 45.1 | 32.9° |
| 3 | 0.932 | 147.6 | 32.5 | 52.6 | 42.7° |
| 4 | 1.000 | 175.0 | 27.4 | 55.0 | 45.8° |
| 5 | 0.909 | 196.8 | 21.8 | 51.8 | 41.7° |

**The spacing collapse is automatic.** Base spacing is 25px, but `amplitude * wave` adds horizontal displacement that grows then shrinks. At i=4, the wave peaks; at i=5, the wave starts to retreat, *pulling that character back toward i=4*. So position 5 sits only 21.8px from position 4, while position 1 sits 38.6px from position 0. With font size 52 in monospace (char width ~28-30px), **the last two characters overlap by definition.** That's where your A+7→4 merger comes from — it's geometrically guaranteed by the math, not an accident.

**Things you got right that I want to highlight:**

1. **`"abcdefghijklmnopqrstuvwxyzABDELQRTY1234567890"`** — this is the smartest line in the file. You kept only the uppercase letters that don't visually collide with lowercase or digits: A, B, D, E, L, Q, R, T, Y. You dropped C/c, I/1/l/i, O/0/o, P/p, S/s, U/u, V/v, W/w, X/x, Z/z, K/k, M/m, N/n. That's a lot of design thought baked into one string literal. **Most CAPTCHAs leak entropy through visually colliding characters and never recover it.**

2. **Single sin() driving everything** — by tying rotation to wave value, you guarantee that the most-rotated characters are also the most-displaced, which is exactly when they collide with neighbors. If rotation and displacement were independent randoms, you'd often get high-rotation characters with normal spacing (readable) and low-rotation characters with collapsed spacing (still readable). Coupling them ensures **the difficulty stacks at the same character positions.**

3. **Font size 52 in 80px height** — pushes characters near the patch-grid limit. With the wave displacement, characters at the wave peak nearly hit the top/bottom borders. Which leads to...

**A subtle thing that might be a happy accident or might be intentional — please confirm:**

At `i=0`, y=20, character center is at 20. With font size 52, the character extends from y=-6 to y=46. **The top of the character is clipped above the image boundary.** At i=4, y=55, character extends from y=29 to y=81 — **clipped at the bottom.**

If I'm right that clipping is happening, this is part of why my "i" detection failed. A rotated "i" near the wave peak might have its **dot clipped off the top of the image**, leaving just the stem — which is then indistinguishable from a noise line. The clipping itself becomes part of the camouflage.

Was that intentional or emergent? Either way, it's working.

**Now the honest critique — there's one major vulnerability:**

```go
amplitude := 35.0
frequency := 0.4
startX := 40.0
spacing := 25.0
```

**These constants are fixed across every CAPTCHA you generate.** That means:

- Character 0 is always at x ≈ 40, rotated 0°
- Character 5 is always at x ≈ 197, rotated ~42°
- The wave shape is identical every time

An attacker who collects ~1000 of your CAPTCHAs can:
1. Crop each character position deterministically (you've told them where every character is)
2. De-rotate by the known angle for each position
3. Run OCR on the de-rotated isolated character against a known font

**This collapses your defense.** The whole reason your design beats current models is that they can't separate characters or correct for rotation. But if the rotation and position are *publicly determined by your code*, an attacker bypasses recognition entirely — they just reverse the transformation.

**Fix — randomize the wave parameters per generation:**

```go
amplitude := 25.0 + rand.Float64()*15.0    // 25–40
frequency := 0.3 + rand.Float64()*0.3      // 0.3–0.6
phase := rand.Float64() * math.Pi * 2      // random phase
startX := 30.0 + rand.Float64()*20.0       // 30–50
spacing := 22.0 + rand.Float64()*8.0       // 22–30
rotationGain := 0.6 + rand.Float64()*0.4   // 0.6–1.0

// then:
wave := math.Sin(frequency*pos + phase)
angle := wave * rotationGain
```

This destroys the position-prediction attack. The attacker no longer knows where character N is, what angle it's at, or how much it overlaps with N+1.

**Other improvements, ranked by impact:**

1. **Draw some lines after characters, not just before.** Currently all 15 lines are drawn first, so characters paint *over* the lines. The lines you see crossing characters in the rendered image are only visible *between* character strokes (not on top of them). To get true overlap-camouflage on the thin character strokes, draw ~5 lines after the character loop. This is the line that turns your noise from "background texture" into "stroke camouflage."

2. **Vary line stroke width.** Right now `dc.Stroke()` uses default width (1px). Mix in some lines at the same stroke width as the font (which at size 52 is roughly 4-5px). This is exactly the principle you described — your noise vocabulary must match your character stroke distribution. Currently it doesn't.

3. **Randomize length 5–7 instead of fixed 6.** Forces attackers to detect length per-image, and adds entropy. With your character set, length 5 still gives 43^5 = 147M combinations — plenty.

4. **Add a few short curve/arc segments to the noise.** Pure straight lines are easy for a model to identify as "noise" structurally (Hough transform finds them). Arcs that match character curves (the round part of "a", "e", "p") would camouflage curved character segments the same way your straight lines camouflage straight strokes.

5. **Consider a small render-then-downsample step.** Render at 480x160, apply slight Gaussian blur, downsample to 240x80. This destroys sub-pixel edge information that some models exploit. Humans don't lose anything because we read by gestalt, not by edge sharpness.

6. **`rand` package — confirm it's `crypto/rand` not `math/rand`.** Your import says `github.com/donnyhardyanto/dxlib/utils/crypto/rand` which sounds crypto-backed, but if `rand.Float64()` ultimately wraps `math/rand` without seeding from a secure source, attackers can predict the sequence. Worth verifying.

7. **`time/tzdata` blank import** — irrelevant to this file, looks like a leftover. Minor cleanup.

**The change that matters most by far is #1 — randomize the wave parameters.** The other six are polish; that one is critical. Without it, your design's mathematical elegance becomes its weakness: deterministic transformations are reversible.

Want me to write up the randomized version as a patch?