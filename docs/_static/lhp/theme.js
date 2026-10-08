/* Furo owns theme persistence and search; this adds the responsive LHP shell. */
(() => {
  const toggle = document.querySelector(".lhp-menu");
  const drawer = document.querySelector(".sidebar-drawer");
  const navigation = document.querySelector("#lhp-navigation");
  const mobile = matchMedia("(max-width: 800px)");
  function setNavigation(open, restoreFocus = false) {
    document.body.classList.toggle("lhp-nav-open", open);
    toggle?.setAttribute("aria-expanded", String(open));
    toggle?.setAttribute(
      "aria-label",
      open ? "Close navigation" : "Open navigation",
    );
    if (drawer) drawer.inert = mobile.matches && !open;
    const main = document.querySelector(".main");
    if (main) main.inert = mobile.matches && open;
    document
      .querySelectorAll(".lhp-header a, .lhp-header input, .lhp-header button")
      .forEach((control) => {
        if (control !== toggle) control.inert = mobile.matches && open;
      });
    if (open) navigation?.querySelector("button, a")?.focus();
    if (restoreFocus) toggle?.focus();
  }
  toggle?.addEventListener("click", () =>
    setNavigation(!document.body.classList.contains("lhp-nav-open")),
  );
  document
    .querySelector(".lhp-close-nav")
    ?.addEventListener("click", () => setNavigation(false, true));
  document
    .querySelector(".sidebar-overlay")
    ?.addEventListener("click", () => setNavigation(false, true));
  navigation
    ?.querySelectorAll("a, [data-agent-dialog]")
    .forEach((link) =>
      link.addEventListener("click", () => setNavigation(false)),
    );
  mobile.addEventListener("change", () => setNavigation(false));
  document.addEventListener("keydown", (event) => {
    if (document.querySelector("dialog[open]")) return;
    if (
      event.key === "Tab" &&
      document.body.classList.contains("lhp-nav-open")
    ) {
      const items = [toggle, ...navigation.querySelectorAll("a, button")];
      const last = items.at(-1);
      if (event.shiftKey && document.activeElement === toggle) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        toggle?.focus();
      }
    }
    if (
      event.key === "Escape" &&
      document.body.classList.contains("lhp-nav-open")
    )
      setNavigation(false, true);
    if (
      event.key === "/" &&
      !event.ctrlKey &&
      !event.metaKey &&
      !event.altKey &&
      !event.target.closest("input, textarea, select, [contenteditable=true]")
    ) {
      const search = document.querySelector("#lhp-search-input");
      if (search?.getClientRects().length) {
        event.preventDefault();
        search.focus();
      }
    }
  });
  setNavigation(false);
})();
