/* Agent dialog adapted from the LHP website; prompts share repository guides. */
(() => {
async function copyText(value) {
    if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(value);
        return;
    }
    const textarea = document.createElement("textarea");
    const previousFocus = document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    textarea.value = value;
    textarea.readOnly = true;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    (document.querySelector("dialog[open]") ?? document.body).appendChild(textarea);
    textarea.select();
    const successful = document.execCommand("copy");
    textarea.remove();
    previousFocus?.focus();
    if (!successful)
        throw new Error("Copy failed");
}
const copyTimers = new WeakMap();
document
    .querySelectorAll(".copy-button")
    .forEach((button) => {
    button.addEventListener("click", async () => {
        const feedbackId = button.dataset.feedback;
        const feedback = feedbackId ? document.getElementById(feedbackId) : null;
        const copiedValue = button.dataset.copy ?? "";
        const copiedVersion = button.dataset.copyVersion;
        try {
            await copyText(copiedValue);
            if ((button.dataset.copy ?? "") !== copiedValue ||
                button.dataset.copyVersion !== copiedVersion)
                return;
            if (feedback)
                feedback.textContent = "Copied to clipboard.";
            const oldTimer = copyTimers.get(button);
            if (oldTimer)
                window.clearTimeout(oldTimer);
            copyTimers.set(button, window.setTimeout(() => {
                if (feedback)
                    feedback.textContent = "";
            }, 2400));
        }
        catch {
            if ((button.dataset.copy ?? "") !== copiedValue ||
                button.dataset.copyVersion !== copiedVersion)
                return;
            if (feedback)
                feedback.textContent = "Unable to copy. Select the text manually.";
        }
    });
});
const agentDialog = document.querySelector("#agent-dialog");
const agentPrompt = document.querySelector("#agent-prompt");
const agentCopyButton = document.querySelector("#agent-copy-prompt");
const agentNames = {
    "genie-code": "Databricks Genie Code",
    claude: "Claude",
    codex: "Codex",
    gemini: "Gemini",
    githubcopilot: "Copilot",
};
let agentDialogMode = "onboard";
let agentDialogTrigger = null;
let previousOverflow = "";
let promptGeneration = 0;
function updateAgentPrompt() {
    if (!agentDialog || !agentPrompt || !agentCopyButton)
        return;
    const agent = agentDialog.querySelector('input[name="agent-choice"]:checked')?.value ?? "genie-code";
    const name = agentNames[agent] ?? "your agent";
    const learning = agentDialogMode === "learn";
    agentDialog
        .querySelectorAll('input[name="agent-goal"]')
        .forEach((radio) => {
        radio.checked = radio.value === agentDialogMode;
    });
    document.querySelector("#agent-prompt-label").textContent =
        `Paste this into ${name}`;
    document.querySelector("#agent-location-help").textContent =
        agent === "genie-code"
            ? "Open Genie Code in your Databricks workspace."
            : `Open ${name} in the folder where you want to work.`;
    document.querySelector("#agent-prompt-help").textContent =
        learning
            ? "Learn from the current LHP documentation with guided examples."
            : "Your agent will ask whether you want a sample or an empty project.";
    document.querySelector("#agent-copy-label").textContent =
        learning ? "Copy learning prompt" : "Copy setup prompt";
    let prompt = learning
        ? `I'm using ${name}. Help me learn Lakehouse Plumber.\n\nRead and follow ${agentDialog.dataset.guidesBase}/learn.md. Use its docs-guided learning route and the current published LHP documentation. Ask about my experience with SQL, Python, and Databricks, and what I want to build. Guide me one step at a time, explaining the YAML and generated Python. Do not claim that a dedicated teaching skill is installed.`
        : `I'm using ${name}. Help me set up Lakehouse Plumber.\n\nRead and follow ${agentDialog.dataset.guidesBase}/agent.md.\n\nUse uv or a Python virtual environment to install LHP. Ask whether I want the sample or an empty project, and ask for the project name and folder. Then scaffold it and install and load the LHP authoring skill using lhp skill install.`;
    if (!learning && agent === "genie-code") {
        prompt += `\n\nFirst confirm where the project should live and which execution tools you have in this Databricks workspace. If you cannot run this setup in a suitable persistent environment, guide me through the commands in my chosen terminal and verify my results.`;
    }
    agentPrompt.value = prompt;
    agentCopyButton.dataset.copy = prompt;
    agentCopyButton.dataset.copyVersion = String(++promptGeneration);
    const timer = copyTimers.get(agentCopyButton);
    if (timer) {
        window.clearTimeout(timer);
        copyTimers.delete(agentCopyButton);
    }
    document.querySelector("#agent-copy-feedback").textContent = "";
}
if (agentDialog && agentPrompt && agentCopyButton) {
    document
        .querySelectorAll("[data-agent-dialog]")
        .forEach((trigger) => {
        trigger.addEventListener("click", () => {
            agentDialogMode =
                trigger.dataset.agentDialog === "learn" ? "learn" : "onboard";
            agentDialogTrigger = trigger;
            updateAgentPrompt();
            previousOverflow = document.documentElement.style.overflow;
            document.documentElement.style.overflow = "hidden";
            agentDialog.showModal();
            agentDialog.scrollTop = 0;
            agentDialog
                .querySelector('input[name="agent-goal"]:checked')
                ?.focus();
        });
    });
    agentDialog
        .querySelectorAll('input[name="agent-goal"]')
        .forEach((radio) => {
        radio.addEventListener("change", () => {
            agentDialogMode = radio.value === "learn" ? "learn" : "onboard";
            updateAgentPrompt();
        });
    });
    agentDialog
        .querySelectorAll('input[name="agent-choice"]')
        .forEach((radio) => radio.addEventListener("change", updateAgentPrompt));
    agentDialog.addEventListener("keydown", (event) => {
        if (event.key !== "Tab")
            return;
        const focusable = Array.from(agentDialog.querySelectorAll('button:not([disabled]), input:not([disabled]), textarea:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])'));
        const first = focusable[0];
        const last = focusable.at(-1);
        if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last?.focus();
        }
        else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first?.focus();
        }
    });
    document
        .querySelector("#agent-dialog-close")
        ?.addEventListener("click", () => agentDialog.close());
    agentDialog.addEventListener("click", (event) => {
        if (event.target !== agentDialog)
            return;
        const rect = agentDialog.getBoundingClientRect();
        if (event.clientX < rect.left ||
            event.clientX > rect.right ||
            event.clientY < rect.top ||
            event.clientY > rect.bottom)
            agentDialog.close();
    });
    agentDialog.addEventListener("close", () => {
        document.documentElement.style.overflow = previousOverflow;
        if (agentDialogTrigger?.isConnected &&
            agentDialogTrigger.getClientRects().length && !agentDialogTrigger.closest("[inert]"))
            agentDialogTrigger.focus();
        else
            document.querySelector(".lhp-menu")?.focus();
    });
}


})();
