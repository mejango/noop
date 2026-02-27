import type { Metadata } from "next";
import { Space_Mono } from "next/font/google";
import "./globals.css";
import Nav from "@/components/Nav";
import AdvisorDrawer from "@/components/AdvisorDrawer";

const spaceMono = Space_Mono({
  weight: ["400", "700"],
  subsets: ["latin"],
  variable: "--font-space-mono",
});

export const metadata: Metadata = {
  title: "NO OPERATION",
  description: "Tail-hedging strategy dashboard",
  icons: {
    icon: "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🥱</text></svg>",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${spaceMono.variable} font-mono antialiased bg-juice-dark text-white min-h-screen`}>
        <Nav />
        <main className="max-w-7xl mx-auto px-3 py-4 md:px-6 md:py-6">
          {children}
        </main>
        <AdvisorDrawer />
      </body>
    </html>
  );
}
