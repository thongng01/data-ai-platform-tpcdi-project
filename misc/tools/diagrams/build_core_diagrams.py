"""Build focused TPC-DI draw.io views and optionally export them to PNG."""
from dataclasses import dataclass
from pathlib import Path
import argparse
import copy
import shutil
import struct
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/TPC-DI/diagrams'
TEMP = ROOT / 'misc/tmp/diagrams'
FONT, MONO = 'Ubuntu Sans', 'Ubuntu Sans Mono'
INK, LINE, BLUE, GREEN = '#183246', '#486477', '#244f72', '#245e54'


@dataclass
class Table:
    ident: str
    x: int
    y: int
    w: int
    h: int
    fields: dict

    def port(self, field, side):
        if side in ('top', 'bottom'):
            return .5, 0 if side == 'top' else 1
        return (0 if side == 'left' else 1), self.fields.get(field, .5)


class Page:
    def __init__(self, book, slug, title, height, color):
        self.slug, self.color, self.count = slug, color, 0
        diagram = ET.SubElement(book, 'diagram', id=slug, name=title)
        model = ET.SubElement(diagram, 'mxGraphModel', grid='1', gridSize='10', page='1',
                              pageScale='1', pageWidth='1140', pageHeight=str(height), background='#FFFFFF')
        self.root = ET.SubElement(model, 'root')
        ET.SubElement(self.root, 'mxCell', id='0')
        ET.SubElement(self.root, 'mxCell', id='1', parent='0')
        self.box('canvas', 0, 0, 1140, height, '', 'fillColor=#FFFFFF;strokeColor=none;')
        self.text(40, 24, 1060, 56, title, 34, color, True)

    def box(self, name, x, y, w, h, value, style, parent='1'):
        ident = f'{self.slug}-{name}'
        cell = ET.SubElement(self.root, 'mxCell', id=ident, value=value, style=style, vertex='1', parent=parent)
        ET.SubElement(cell, 'mxGeometry', x=str(x), y=str(y), width=str(w), height=str(h), **{'as': 'geometry'})
        return ident

    def text(self, x, y, w, h, value, size=18, color=INK, bold=False, parent='1', align='left', mono=False):
        self.count += 1
        return self.box(f'text-{self.count}', x, y, w, h, value,
                        f'text;html=0;whiteSpace=wrap;overflow=hidden;align={align};verticalAlign=middle;'
                        f'spacing=0;fontFamily={MONO if mono else FONT};fontSize={size};fontColor={color};fontStyle={int(bold)};', parent)

    def table(self, name, x, y, title, meaning, fields, width=400):
        header, description, row = 48, 64, 36
        height = header + description + row * len(fields) + 8
        parent = self.box(name, x, y, width, height, '', f'fillColor=#FFFFFF;strokeColor={self.color};strokeWidth=1.5;')
        self.box(name+'-header', 0, 0, width, header, '', f'fillColor={self.color};strokeColor=none;', parent)
        self.text(16, 0, width-32, header, title, 23, '#FFFFFF', True, parent)
        self.text(16, header+4, width-32, description-8, meaning, 18, INK, parent=parent)
        positions = {}
        type_width = 160 if any(len(dtype) > 12 for _, _, dtype in fields) else 100
        for i, (mark, field, dtype) in enumerate(fields):
            yy = header + description + i * row
            fill = '#eef3f6' if i % 2 == 0 else '#FFFFFF'
            self.box(f'{name}-row-{i}', 1, yy, width-2, row, '', f'fillColor={fill};strokeColor=none;', parent)
            self.text(12, yy, 36, row, mark, 14, self.color, True, parent)
            self.text(52, yy, width-type_width-74, row, field, 17, INK, bool(mark), parent, mono=True)
            self.text(width-type_width-16, yy, type_width, row, dtype, 16, LINE, parent=parent, align='right', mono=True)
            positions[field] = (yy + row/2) / height
        return Table(parent, x, y, width, height, positions)

    def note(self, name, x, y, w, h, title, body):
        parent = self.box(name, x, y, w, h, '', 'fillColor=#eef3f6;strokeColor=#c7d5df;')
        self.text(16, 8, w-32, 30, title, 22, self.color, True, parent)
        self.text(16, 42, w-32, h-50, body, 18, INK, parent=parent)

    def edge(self, source, sf, target, tf, ss='right', ts='left', via_x=None, via_y=None,
             er=False, optional=False, label='', sp=None, tp=None, points=None, label_offset=None):
        self.count += 1
        sx, sy = sp or source.port(sf, ss)
        tx, ty = tp or target.port(tf, ts)
        style = (f'edgeStyle=none;rounded=0;html=0;exitPerimeter=0;entryPerimeter=0;exitX={sx};exitY={sy};'
                 f'entryX={tx};entryY={ty};exitDx=0;exitDy=0;entryDx=0;entryDy=0;strokeColor={LINE};'
                 f'strokeWidth=2;fontFamily={FONT};fontSize=17;fontColor={INK};'
                 'labelBackgroundColor=#FFFFFF;jumpStyle=arc;jumpSize=7;')
        if er:
            style += f'startArrow={"ERzeroToOne" if optional else "ERmandOne"};endArrow=ERzeroToMany;startFill=0;endFill=0;startSize=16;endSize=16;'
        else:
            style += 'startArrow=none;endArrow=open;endFill=0;endSize=12;'
        cell = ET.Element('mxCell', id=f'{self.slug}-edge-{self.count}', value=label, source=source.ident,
                          target=target.ident, parent='1', edge='1', style=style)
        geo = ET.SubElement(cell, 'mxGeometry', relative='1', **{'as': 'geometry'})
        if label_offset:
            ET.SubElement(geo, 'mxPoint', x=str(label_offset[0]), y=str(label_offset[1]), **{'as': 'offset'})
        if points is None:
            points = []
            if via_x is not None:
                points = [(via_x, source.y + sy*source.h), (via_x, target.y + ty*target.h)]
            elif via_y is not None:
                points = [(source.x + sx*source.w, via_y), (target.x + tx*target.w, via_y)]
        if points:
            array = ET.SubElement(geo, 'Array', **{'as': 'points'})
            for x, y in points:
                ET.SubElement(array, 'mxPoint', x=str(x), y=str(y))
        self.root.insert(3, cell)


def input_accounts(book):
    p = Page(book, 'tpc-di-input-accounts', 'Customer and account inputs', 1060, BLUE)
    p.note('xml',40,100,1060,102,'CustomerMgmt.xml',
           'Batch1 supplies timed customer and account actions in XML.\nCustomer.txt and Account.txt supply the later daily changes.')
    c = p.table('customer',40,260,'Customer.txt','Each row changes a customer\nin Batch2 or Batch3.',[
        ('ID','C_ID','IDENT_T'),('','C_TAX_ID','CHAR(20)'),('','C_L_NAME','CHAR(25)'),('','C_F_NAME','CHAR(20)')])
    b = p.table('hr',700,260,'HR.csv','Each row describes an employee.\nJob code 314 identifies a broker.',[
        ('ID','EmployeeID','IDENT_T'),('','EmployeeJobCode','NUM(3)'),('','EmployeeLastName','CHAR(30)')])
    a = p.table('account',40,650,'Account.txt','Each row changes an account\nin Batch2 or Batch3.',[
        ('ID','CA_ID','IDENT_T'),('REF','CA_C_ID','IDENT_T'),('REF','CA_B_ID','IDENT_T'),('','CA_NAME','CHAR(50)'),('','CA_ST_ID','CHAR(4)')])
    t = p.table('trade',700,650,'Trade.txt','Batch1 gives trade states.\nLater batches give trade changes.',[
        ('ID','T_ID','TRADE_T'),('REF','T_CA_ID','IDENT_T'),('REF','T_S_SYMB','CHAR(15)'),('','T_DTS','DATETIME'),('','T_ST_ID','CHAR(4)')])
    p.edge(a,'CA_C_ID',c,'C_ID',ss='left',ts='left',via_x=16)
    p.edge(a,'CA_B_ID',b,'EmployeeID',ss='top',ts='bottom',via_y=600,label='CA_B_ID')
    p.edge(t,'T_CA_ID',a,'CA_ID',ss='left',ts='right',via_x=570)
    return p


def input_market(book):
    p = Page(book,'tpc-di-input-market','Company, market, and trade inputs',1270,BLUE)
    c = p.table('company',40,120,'FINWIRE CMP','Each row describes a posted\ncompany record.',[
        ('ID','CIK','CHAR(10)'),('','CompanyName','CHAR(60)'),('','PTS','CHAR(15)')])
    s = p.table('security',700,120,'FINWIRE SEC','Each row describes a posted\nsecurity record.',[
        ('ID','Symbol','CHAR(15)'),('REF','CoNameOrCIK','CHAR(60 or 10)'),('','PTS','CHAR(15)')])
    f = p.table('financial',40,500,'FINWIRE FIN','Each row reports a company\nquarter.',[
        ('REF','CoNameOrCIK','CHAR(60 or 10)'),('','Year','CHAR(4)'),('','Quarter','CHAR(1)'),('','EPS','CHAR(12)')])
    m = p.table('market',700,500,'DailyMarket.txt','Each row gives market measures\nfor one security and day.',[
        ('REF','DM_S_SYMB','CHAR(15)'),('','DM_DATE','DATE'),('','DM_CLOSE','S_PRICE_T'),('','DM_VOL','S_COUNT_T')])
    t = p.table('trade',40,880,'Trade.txt','The trade identifies an account\nand a security.',[
        ('ID','T_ID','TRADE_T'),('REF','T_S_SYMB','CHAR(15)'),('','T_DTS','DATETIME'),('','T_ST_ID','CHAR(4)')])
    h = p.table('history',700,880,'TradeHistory.txt','Each row records a trade status\nchange in Batch1.',[
        ('REF','TH_T_ID','TRADE_T'),('','TH_DTS','DATETIME'),('','TH_ST_ID','CHAR(4)')])
    p.edge(s,'CoNameOrCIK',c,'CompanyName',ss='left',ts='right',label='Name or CIK',label_offset=(-55,-22))
    p.edge(f,'CoNameOrCIK',c,None,ss='left',ts='left',via_x=16)
    p.edge(m,'DM_S_SYMB',s,'Symbol',ts='right',via_x=1124)
    p.edge(t,'T_S_SYMB',s,'Symbol',via_x=570)
    p.edge(h,'TH_T_ID',t,'T_ID',ss='left',ts='right')
    return p


def warehouse_accounts(book):
    p = Page(book,'tpc-di-warehouse-accounts','Customer and trade dimensions',1240,GREEN)
    c = p.table('customer',60,120,'DimCustomer','Each row holds one dated\nversion of a customer.',[
        ('PK','SK_CustomerID','SK_T'),('BK','CustomerID','IDENT_T'),('','LastName','CHAR(30)')])
    b = p.table('broker',680,120,'DimBroker','Each row describes a broker\nfrom the HR snapshot.',[
        ('PK','SK_BrokerID','SK_T'),('BK','BrokerID','IDENT_T'),('','LastName','CHAR(50)')])
    a = p.table('account',370,490,'DimAccount','Each row holds one dated\nversion of an account.',[
        ('PK','SK_AccountID','SK_T'),('BK','AccountID','IDENT_T'),('FK','SK_CustomerID','SK_T'),('FK','SK_BrokerID','SK_T')])
    t = p.table('trade',370,870,'DimTrade','Each trade has one row.\nLater changes update that row.',[
        ('PK','TradeID','IDENT_T'),('FK','SK_AccountID','SK_T'),('FK','SK_CustomerID','SK_T'),('FK','SK_BrokerID','SK_T'),('','Quantity','NUM(6,0)'),('','TradePrice','NUM(8,2)')])
    p.edge(c,'SK_CustomerID',a,'SK_CustomerID',ss='bottom',via_x=260,er=True)
    p.edge(b,'SK_BrokerID',a,'SK_BrokerID',ss='bottom',ts='right',via_x=880,er=True)
    p.edge(a,'SK_AccountID',t,'SK_AccountID',ss='bottom',ts='top',er=True)
    p.edge(c,'SK_CustomerID',t,'SK_CustomerID',ss='left',via_x=16,er=True)
    p.edge(b,'SK_BrokerID',t,'SK_BrokerID',ts='right',via_x=1124,er=True,optional=True)
    return p


def warehouse_market(book):
    p = Page(book,'tpc-di-warehouse-market','Company and market tables',970,GREEN)
    c = p.table('company',60,120,'DimCompany','Each row holds one dated\nversion of a company.',[
        ('PK','SK_CompanyID','SK_T'),('BK','CompanyID','IDENT_T'),('','Name','CHAR(60)')])
    s = p.table('security',680,120,'DimSecurity','Each row holds one dated\nversion of a security.',[
        ('PK','SK_SecurityID','SK_T'),('BK','Symbol','CHAR(15)'),('FK','SK_CompanyID','SK_T'),('','Dividend','VALUE_T')])
    f = p.table('financial',60,570,'Financial','Each row reports one company\nquarter.',[
        ('FK','SK_CompanyID','IDENT_T'),('','FI_YEAR','NUM(4)'),('','FI_QTR','NUM(1)'),('','FI_BASIC_EPS','SNUM(10,2)')])
    m = p.table('market',680,570,'FactMarketHistory','Each row gives market measures\nfor one security and date.',[
        ('FK','SK_SecurityID','SK_T'),('FK','SK_CompanyID','SK_T'),('FK','SK_DateID','SK_T'),('','ClosePrice','NUM(8,2)'),('','PERatio','NUM(10,2)'),('','Yield','NUM(5,2)')])
    p.edge(c,'SK_CompanyID',s,'SK_CompanyID',via_x=600,er=True)
    p.edge(c,'SK_CompanyID',f,'SK_CompanyID',ss='bottom',ts='top',er=True)
    p.edge(c,'SK_CompanyID',m,'SK_CompanyID',sp=(.8,1),
           points=[(380,450),(550,450),(550,736)],er=True)
    p.edge(s,'SK_SecurityID',m,'SK_SecurityID',ts='right',via_x=1124,er=True)
    return p


def warehouse_activity(book):
    p = Page(book,'tpc-di-warehouse-activity','Cash, holding, and watch records',1220,GREEN)
    p.table('cash',40,120,'FactCashBalances','Each row holds the balance after\nan account has cash changes that day.',[
        ('FK','SK_CustomerID','SK_T'),('FK','SK_AccountID','SK_T'),('FK','SK_DateID','SK_T'),('','Cash','SNUM(15,2)')],480)
    p.note('cash-keys',700,120,400,264,'Dimension references',
           'SK_CustomerID refers to DimCustomer.\n\nSK_AccountID refers to DimAccount.\n\nSK_DateID refers to DimDate.')
    h = p.table('holding',40,500,'FactHoldings','Each row records a change to\na holding caused by a trade.',[
        ('FK','TradeID','IDENT_T'),('FK','CurrentTradeID','IDENT_T'),('','CurrentPrice','S_PRICE_T'),('','CurrentHolding','SNUM(6)')],480)
    t = p.table('trade',700,500,'DimTrade','Both trade identifiers refer\nto this table.',[('PK','TradeID','IDENT_T'),('','TradePrice','NUM(8,2)')])
    p.edge(t,'TradeID',h,'TradeID',ss='left',ts='right',er=True)
    p.edge(t,'TradeID',h,'CurrentTradeID',ss='bottom',ts='right',
           points=[(900,810),(610,810),(610,666)],er=True)
    p.table('watch',40,880,'FactWatches','Each row records when a customer\nwatches a security.',[
        ('FK','SK_CustomerID','SK_T'),('FK','SK_SecurityID','SK_T'),('FK','SK_DateID_DatePlaced','SK_T'),('FK','SK_DateID_DateRemoved','SK_T')],600)
    p.note('watch-keys',760,880,340,264,'Dimension references',
           'Customer and security keys select the versions at placement.\n\nBoth date keys refer to DimDate. The removal key can be NULL.')
    return p


def write_views(book, pages, source, export=False):
    OUT.mkdir(parents=True, exist_ok=True)
    ET.indent(book,space='  ')
    ET.ElementTree(book).write(source,encoding='utf-8',xml_declaration=True)
    for diagram in book.findall('diagram'):
        single = ET.Element('mxfile', **book.attrib)
        single.append(copy.deepcopy(diagram))
        ET.ElementTree(single).write(OUT / (diagram.get('id') + '.drawio'), encoding='utf-8', xml_declaration=True)
    print(f'Wrote {source.relative_to(ROOT)} with {len(pages)} views.')
    if not export:
        return
    executable = shutil.which('drawio')
    if not executable:
        raise SystemExit('Install Linux draw.io and put drawio on PATH before exporting.')
    TEMP.mkdir(parents=True,exist_ok=True)
    for index,page in enumerate(pages,1):
        preview = TEMP/f'{page.slug}.png'
        preview.unlink(missing_ok=True)
        result = subprocess.run([executable,'--export','--format','png','--page-index',str(index),
                                 '--scale','1.5','--output',str(preview),str(source)],capture_output=True,text=True,timeout=60)
        (TEMP/f'{page.slug}.log').write_text(result.stdout+result.stderr,encoding='utf-8')
        if result.returncode:
            raise SystemExit(f'Export failed for {page.slug}; read its log in {TEMP}.')
        data = preview.read_bytes()
        if data[:8] != b'\x89PNG\r\n\x1a\n' or min(struct.unpack('>II',data[16:24])) < 100:
            raise SystemExit(f'Invalid PNG: {preview}')
        shutil.copy2(preview,OUT/preview.name)
        print(f'Exported {preview.name}.')


def build(export=False):
    book = ET.Element('mxfile',host='app.diagrams.net',type='device',version='32.6.1')
    pages = [fn(book) for fn in (input_accounts,input_market,warehouse_accounts,warehouse_market,warehouse_activity)]
    write_views(book, pages, OUT/'tpc-di-core-model.drawio', export)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export',action='store_true',help='Export all views with Linux draw.io.')
    build(parser.parse_args().export)
