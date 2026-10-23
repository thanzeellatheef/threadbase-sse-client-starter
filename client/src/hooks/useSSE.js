
import { useEffect } from "react";
import { useAuth } from "../auth/AuthContext.jsx";

export function useSSE(url, onMessage) {
  const { user, token } = useAuth();

  useEffect(() => {
    if (!user || !token) return;

    const controller = new AbortController();

    async function connect() {
      try {
        const response = await fetch(url, {
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: "text/event-stream",
          },
          signal: controller.signal,
        });

        if (!response.ok) {
          throw new Error(`SSE connection failed: ${response.status}`);
        }

        if (!response.body) {
          throw new Error("SSE response has no readable stream");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { value, done } = await reader.read();

          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          buffer = buffer.replace(/\r\n/g, "\n");

          const events = buffer.split("\n\n");
          buffer = events.pop() ?? "";

          for (const event of events) {
            const data = event
              .split("\n")
              .filter((line) => line.startsWith("data:"))
              .map((line) => line.slice(5).trimStart())
              .join("\n");

            if (!data) continue;

            try {
              onMessage(JSON.parse(data));
            } catch (error) {
              console.error("Error processing SSE message:", error);
            }
          }
        }
      } catch (error) {
        if (error.name !== "AbortError") {
          console.error("SSE connection error:", error);
        }
      }
    }

    connect();

    return () => controller.abort();
  }, [url, user, token, onMessage]);
}
