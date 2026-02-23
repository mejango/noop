export default function Badge({ label, color = 'zinc' }: { label: string; color?: string }) {
  const colors: Record<string, string> = {
    green: 'bg-green-900/40 text-green-400 border-green-800/50',
    red: 'bg-red-900/40 text-red-400 border-red-800/50',
    yellow: 'bg-yellow-900/40 text-yellow-400 border-yellow-800/50',
    blue: 'bg-blue-900/40 text-blue-400 border-blue-800/50',
    zinc: 'bg-zinc-800 text-zinc-400 border-zinc-700',
    gray: 'bg-zinc-800 text-zinc-500 border-zinc-700',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${colors[color] || colors.zinc}`}>
      {label}
    </span>
  );
}
