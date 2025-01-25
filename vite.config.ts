import { defineConfig } from "vite";
import preact from "@preact/preset-vite";
import * as path from "path";

export default defineConfig({
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: { outDir: "../dist", emptyOutDir: true },
  plugins: [preact()],
});
