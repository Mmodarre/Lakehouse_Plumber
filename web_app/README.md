# LHP Web

The local web IDE for [Lakehouse Plumber](../README.md) — a React + TypeScript + Vite
single-page app served by the FastAPI backend behind `lhp web`.

## Development

```bash
npm install
npm run dev        # Vite dev server on http://localhost:5173
```

The dev server proxies `/api` to the LHP backend at `http://localhost:8000`
(see `vite.config.ts`). Start the backend from the repo root with the Python
environment active:

```bash
pip install -e ".[webapp]"
lhp web --no-open
```

## Scripts

| Command | Purpose |
|---|---|
| `npm run dev` | Vite dev server with HMR (proxies `/api` to `:8000`) |
| `npm run build` | Type-check (`tsc -b`) + production build to `dist/` |
| `npm run lint` | ESLint |
| `npm run gen:api` | Regenerate `src/types/api.generated.ts` from the FastAPI OpenAPI schema (requires the repo's Python env) |
| `npm run preview` | Preview the production build |

## Shipping the SPA

Source and git installs of `lhp` ship without a prebuilt SPA. To embed the
build into the Python package, run `scripts/build_webapp.sh` from the repo
root — it builds `dist/` and copies it into `src/lhp/webapp/static/`.

## Styling

Tailwind CSS 4 with an oklch design-token palette defined in `src/index.css`
(light + dark, toggled via a `dark` class on `<html>`), shadcn/ui primitives
in `src/components/ui/`, and lucide-react icons. Use the semantic token
utilities (`bg-card`, `text-muted-foreground`, `text-kind-*`, `bg-success`, …)
instead of raw Tailwind palette colors.
