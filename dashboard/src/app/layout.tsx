import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Stream-Table OLAP Engine",
  description:
    "Real-time stream-table join and OLAP engine dashboard with sub-second analytics",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-gray-950 text-gray-100 antialiased">
        {children}
      </body>
    </html>
  );
}
