export default function Card({ title, subtitle, children, className = '' }: { title?: string; subtitle?: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`glass p-4 ${className}`}>
      {title && (
        <div className="mb-2">
          <h3 className="text-sm font-semibold text-juice-orange">{title}</h3>
          {subtitle && <p className="text-xs text-gray-500">{subtitle}</p>}
        </div>
      )}
      {children}
    </div>
  );
}
