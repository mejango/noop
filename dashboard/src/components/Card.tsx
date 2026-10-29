export default function Card({ title, subtitle, children, className = '' }: { title?: string; subtitle?: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`glass relative p-4 ${className}`}>
      {title && (
        <div className="mb-2">
          <h3 className="tui-title text-sm font-bold text-juice-orange">{title}</h3>
          {subtitle && <p className="text-xs text-gray-500">{subtitle}</p>}
        </div>
      )}
      {children}
    </div>
  );
}
