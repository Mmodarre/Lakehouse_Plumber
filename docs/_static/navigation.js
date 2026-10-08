// Keep the current page in view inside the long, fully expanded menu.
document.addEventListener("DOMContentLoaded", () => {
  const scroller = document.querySelector(".sidebar-scroll");
  const current = document.querySelector(".sidebar-tree .current-page > a");
  if (!scroller || !current) return;

  current.setAttribute("aria-current", "page");
  const revealCurrentPage = () => {
    const viewport = scroller.getBoundingClientRect();
    const item = current.getBoundingClientRect();
    if (item.top < viewport.top || item.bottom > viewport.bottom) {
      scroller.scrollTop += item.top - viewport.top - Math.min(120, scroller.clientHeight / 3);
    }
  };

  requestAnimationFrame(revealCurrentPage);
  document.querySelector("#__navigation")?.addEventListener("change", (event) => {
    if (event.target.checked) revealCurrentPage();
  });
});
