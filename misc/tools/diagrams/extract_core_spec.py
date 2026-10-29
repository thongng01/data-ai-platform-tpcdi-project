from pathlib import Path
from pypdf import PdfReader
src = Path('docs/TPC-DI/TPC-DI_v1.1.0.pdf')
reader = PdfReader(src)
nums = [21,22,25,26,27,28,29,30,31,33,34,38,39,40,41,42]
parts = [f'\n=== SPECIFICATION PAGE {n} ===\n' + reader.pages[n-1].extract_text(extraction_mode='layout') for n in nums]
out = Path('misc/tmp/diagrams/core-spec-pages.txt')
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text('\n'.join(parts), encoding='utf-8')
print(f'Extracted {len(nums)} pages to {out}')
