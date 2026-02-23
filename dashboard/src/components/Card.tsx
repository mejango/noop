export default function Card({ title, children, className = '' }: { title?: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`rounded-lg border border-zinc-800 bg-zinc-900/50 p-4 ${className}`}>
      {title && <h3 className="text-sm font-medium text-zinc-400 mb-3">{title}</h3>}
      {children}
    </div>
  );
}
