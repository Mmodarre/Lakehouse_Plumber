// Type declarations for Monaco Editor ESM sub-modules.
// These modules exist at runtime but don't ship their own .d.ts files.
// The main entry point re-exports the core API, so we declare it here.
declare module 'monaco-editor/esm/vs/editor/editor.api' {
  export * from 'monaco-editor'
}

// Language contribution side-effect modules (no exports)
declare module 'monaco-editor/esm/vs/basic-languages/yaml/yaml.contribution' {}
declare module 'monaco-editor/esm/vs/basic-languages/python/python.contribution' {}
declare module 'monaco-editor/esm/vs/basic-languages/sql/sql.contribution' {}
declare module 'monaco-editor/esm/vs/basic-languages/markdown/markdown.contribution' {}
declare module 'monaco-editor/esm/vs/basic-languages/shell/shell.contribution' {}
declare module 'monaco-editor/esm/vs/basic-languages/ini/ini.contribution' {}
declare module 'monaco-editor/esm/vs/language/json/monaco.contribution' {}
