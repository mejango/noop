import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "var(--background)",
        foreground: "var(--foreground)",
        "juice-orange": "#F5A623",
        "juice-cyan": "#5CEBDF",
        "juice-dark": "#1a1a1a",
      },
      fontFamily: {
        sans: ["var(--font-jetbrains)", "Menlo", "monospace"],
        mono: ["var(--font-jetbrains)", "Menlo", "monospace"],
      },
    },
  },
  plugins: [require("@tailwindcss/typography")],
};
export default config;
