import type { Metadata } from "next";
import { JetBrains_Mono } from "next/font/google";
import "./globals.css";
import Nav from "@/components/Nav";

const jetbrainsMono = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-jetbrains",
});

export const metadata: Metadata = {
  title: "noop-c",
  description: "Tail-hedging strategy dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${jetbrainsMono.variable} font-mono antialiased bg-juice-dark text-white min-h-screen`}>
        <Nav />
        <main className="max-w-7xl mx-auto px-6 py-6 animate-fade-in">
          {children}
        </main>
      </body>
    </html>
  );
}
