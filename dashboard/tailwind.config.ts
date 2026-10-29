import type { Config } from "tailwindcss";
import colors from "tailwindcss/colors";

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
        // btop-style terminal palette; token names kept so call sites stay unchanged.
        gray: colors.neutral,
        "juice-orange": "#d7c46c",
        "juice-cyan": "#5f9ad7",
        "juice-dark": "#000000",
      },
      fontFamily: {
        sans: ["var(--font-mono)", "Menlo", "monospace"],
        mono: ["var(--font-mono)", "Menlo", "monospace"],
      },
    },
  },
  plugins: [require("@tailwindcss/typography")],
};
export default config;
