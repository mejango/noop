'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const links = [
  { href: '/', label: 'Overview' },
  { href: '/prices', label: 'Prices' },
  { href: '/positions', label: 'Positions' },
  { href: '/options', label: 'Options' },
  { href: '/onchain', label: 'On-Chain' },
  { href: '/trades', label: 'Trades' },
];

export default function Nav() {
  const pathname = usePathname();
  return (
    <nav className="border-b border-zinc-800 bg-zinc-950 px-6 py-3">
      <div className="flex items-center gap-8 max-w-7xl mx-auto">
        <span className="text-lg font-bold tracking-tight text-zinc-100">noop-c</span>
        <div className="flex gap-1">
          {links.map(l => (
            <Link
              key={l.href}
              href={l.href}
              className={`px-3 py-1.5 rounded text-sm transition-colors ${
                pathname === l.href
                  ? 'bg-zinc-800 text-zinc-100'
                  : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50'
              }`}
            >
              {l.label}
            </Link>
          ))}
        </div>
      </div>
    </nav>
  );
}
