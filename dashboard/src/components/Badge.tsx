export default function Badge({ label, color = 'default' }: { label: string; color?: string }) {
  const colors: Record<string, string> = {
    green: 'bg-emerald-900/30 text-emerald-400 border-emerald-500/30',
    red: 'bg-red-900/30 text-red-400 border-red-500/30',
    yellow: 'bg-amber-900/30 text-amber-400 border-amber-500/30',
    blue: 'bg-cyan-900/30 text-juice-cyan border-cyan-500/30',
    zinc: 'bg-white/5 text-gray-400 border-white/10',
    gray: 'bg-white/5 text-gray-500 border-white/10',
    default: 'bg-white/5 text-gray-400 border-white/10',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${colors[color] || colors.default}`}>
      {label}
    </span>
  );
}
