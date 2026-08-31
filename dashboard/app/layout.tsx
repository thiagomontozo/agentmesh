import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AgentMesh Control Room",
  description: "Operational view of Agents, Runs, Workflows, approvals and live events.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
