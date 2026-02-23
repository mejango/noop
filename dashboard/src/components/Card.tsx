export default function Card({ title, children, className = '' }: { title?: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`rounded-lg glass p-4 ${className}`}>
      {title && <h3 className="text-sm font-semibold text-juice-orange mb-3">{title}</h3>}
      {children}
    </div>
  );
}
