import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
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
        "juice-dark-lighter": "#2a2a2a",
      },
      fontFamily: {
        sans: ["var(--font-jetbrains)", "Menlo", "monospace"],
        mono: ["var(--font-jetbrains)", "Menlo", "monospace"],
      },
      animation: {
        "fade-in": "fade-in 0.3s ease-out forwards",
        "pulse-slow": "pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite",
      },
      keyframes: {
        "fade-in": {
          "0%": { opacity: "0", transform: "translateY(4px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
      },
    },
  },
  plugins: [],
};
export default config;
