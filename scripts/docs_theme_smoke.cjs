// Browser smoke test against a built Sphinx site. See scripts/docs-theme.md.
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
(async () => {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({
    permissions: ["clipboard-read", "clipboard-write"],
    colorScheme: "light",
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  const base =
    (process.env.DOCS_BASE_URL || "http://127.0.0.1:8767").replace(/\/$/, "") +
    "/";
  for (const width of [1440, 1280, 1024, 810, 800, 390, 360]) {
    await page.setViewportSize({ width, height: 900 });
    for (const route of [
      "",
      "get-started/01-install-and-scaffold.html",
      "reference/actions/load/cloudfiles.html",
      "reference/config/project.html",
    ]) {
      await page.goto(base + route);
      await page.evaluate(() => document.fonts.ready);
      assert(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        `Overflow ${width}: ${route}`,
      );
      assert(
        (await page.locator(".sidebar-tree a").count()) >= 128,
        "Existing navigation must remain available",
      );
      if (width > 800 && route) {
        const visible = await page.evaluate(() => {
          const link = document.querySelector(".current-page>a");
          const r = link.getBoundingClientRect();
          return r.top >= 72 && r.bottom <= innerHeight;
        });
        assert(visible, `active nav ${route}`);
      }
    }
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base);
  await page.locator(".sd-tab-set>label").nth(2).click();
  await page.locator(".sd-tab-content").nth(2).locator(".copybtn").click();
  assert.equal(
    (await page.evaluate(() => navigator.clipboard.readText())).trim(),
    "lhp validate --env dev\nlhp generate --env dev",
  );
  await page.keyboard.press("/");
  assert.equal(
    await page
      .locator("#lhp-search-input")
      .evaluate((e) => e === document.activeElement),
    true,
  );
  await page.locator("#lhp-search-input").fill("scaffold");
  await page.locator("#lhp-search-input").press("Enter");
  await page.waitForSelector("#search-results a");
  assert(
    (await page.locator("#search-results").innerText()).includes(
      "Install and scaffold",
    ),
  );
  await page.goto(base + "get-started/01-install-and-scaffold.html");
  await page.locator(".lhp-header .theme-toggle").click();
  assert.equal(await page.locator("body").getAttribute("data-theme"), "dark");
  await page.reload();
  assert.equal(await page.locator("body").getAttribute("data-theme"), "dark");
  await page.locator("#docs-start-agent").click();
  for (const goal of ["onboard", "learn"])
    for (const agent of [
      "genie-code",
      "claude",
      "codex",
      "gemini",
      "githubcopilot",
    ]) {
      await page
        .locator(`label:has(input[name=agent-goal][value=${goal}])`)
        .click();
      await page
        .locator(`label:has(input[name=agent-choice][value=${agent}])`)
        .click();
      const prompt = await page.locator("#agent-prompt").inputValue();
      assert(
        prompt.includes(
          "/main/docs/_agent_guides/" +
            (goal === "learn" ? "learn.md" : "agent.md"),
        ),
      );
      assert(
        prompt.includes(
          goal === "learn"
            ? "SQL, Python, and Databricks"
            : "lhp skill install",
        ),
      );
      await page.locator("#agent-copy-prompt").click();
      await page.waitForFunction(() =>
        document
          .querySelector("#agent-copy-feedback")
          .textContent.includes("Copied"),
      );
      assert.equal(
        await page.evaluate(() => navigator.clipboard.readText()),
        prompt,
      );
    }
  await page.locator("#agent-copy-prompt").focus();
  await page.keyboard.press("Tab");
  assert.equal(
    await page.evaluate(() => document.activeElement.id),
    "agent-dialog-close",
  );
  await page.keyboard.press("Escape");
  assert.equal(
    await page.evaluate(() => document.activeElement.id),
    "docs-start-agent",
  );
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator(".lhp-menu").click();
  assert.equal(
    await page.locator(".lhp-menu").getAttribute("aria-expanded"),
    "true",
  );
  await page.locator(".lhp-nav-mobile [data-agent-dialog]").click();
  await page.keyboard.press("Escape");
  await page.waitForFunction(() => document.activeElement.matches(".lhp-menu"));
  await page.locator(".lhp-menu").click();
  await page.keyboard.press("Escape");
  assert.equal(
    await page.locator(".lhp-menu").getAttribute("aria-expanded"),
    "false",
  );
  await page.locator("#docs-start-agent").click();
  await page.locator("#agent-dialog-close").click();
  await page.locator(".lhp-mobile-search").click();
  await page.locator("#lhp-page-query").fill("scaffold");
  await page.locator(".lhp-page-search button").click();
  await page.waitForSelector("#search-results a");
  assert(
    (await page.locator("#search-results").innerText()).includes(
      "Install and scaffold",
    ),
  );
  assert.deepEqual(errors, []);
  console.log(
    "PASS: 28 responsive pages, navigation, native search, code copy, theme persistence, 10 agent prompts, keyboard/dialog/mobile controls",
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
