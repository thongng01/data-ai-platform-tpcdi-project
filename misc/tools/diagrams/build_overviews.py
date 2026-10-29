"""Build TPC-DI data-flow, input, and warehouse overview diagrams."""
import argparse
import xml.etree.ElementTree as ET
from build_core_diagrams import Page, Table, OUT, BLUE, GREEN, INK, write_views


class Overview(Page):
    def card(self, name, x, y, w, h, title, body, color=None):
        color = color or self.color
        parent = self.box(name, x, y, w, h, '', f'fillColor=#FFFFFF;strokeColor={color};strokeWidth=1.5;')
        self.box(name+'-head', 0, 0, w, 42, '', f'fillColor={color};strokeColor=none;', parent)
        self.text(14, 0, w-28, 42, title, 22, '#FFFFFF', True, parent)
        self.text(14, 48, w-28, h-56, body, 18, INK, parent=parent)
        return Table(parent, x, y, w, h, {})


def data_flow(book):
    p = Overview(book, 'tpc-di-data-flow', 'TPC-DI data flow', 1510, BLUE)
    for x, label in [(40, 'Extracted files'), (440, 'Integration work'), (800, 'Warehouse tables')]:
        p.text(x, 100, 300, 32, label, 22, INK, True)
    rows = [
        ('Reference data', 'Date.txt, Time.txt\nIndustry.txt, StatusType.txt\nTaxRate.txt, TradeType.txt',
         'Load shared lookups', 'Load calendar values,\ncode descriptions,\nrates, and flags.',
         'Reference tables', 'DimDate, DimTime\nIndustry, StatusType\nTaxRate, TradeType'),
        ('Customers and accounts', 'CustomerMgmt.xml\nCustomer.txt, Account.txt\nHR.csv, Prospect.csv',
         'Combine and enrich', 'Apply changes, match\nprospects, and keep\ndaily versions.',
         'Customer tables', 'DimCustomer, DimAccount\nDimBroker, Prospect'),
        ('Company information', 'FINWIRE files contain\nCMP, SEC, and FIN records.',
         'Resolve companies', 'Match company identifiers\nand keep dated company\nand security versions.',
         'Company tables', 'DimCompany, DimSecurity\nFinancial'),
        ('Trade records', 'Trade.txt\nTradeHistory.txt',
         'Build trade state', 'Resolve dimension keys\nand combine trade\nattributes and times.',
         'Trade table', 'DimTrade'),
        ('Activity records', 'CashTransaction.txt\nHoldingHistory.txt\nDailyMarket.txt\nWatchHistory.txt',
         'Calculate facts', 'Carry balances forward,\nresolve holdings, calculate\nmarket measures, and\nmatch watch actions.',
         'Fact tables', 'FactCashBalances\nFactHoldings\nFactMarketHistory\nFactWatches'),
        ('Audit records', '*_audit.csv',
         'Load observations', 'Copy the generated\naudit observations.',
         'Audit table', 'Audit'),
    ]
    for i, values in enumerate(rows):
        y = 150 + 200*i
        a = p.card(f'input-{i}',40,y,310,160,values[0],values[1],BLUE)
        b = p.card(f'process-{i}',440,y,280,160,values[2],values[3],'#4b6072')
        c = p.card(f'output-{i}',800,y,300,160,values[4],values[5],GREEN)
        p.edge(a,None,b,None)
        p.edge(b,None,c,None)
    p.note('shared',40,1360,1060,110,'Shared load information',
           'BatchDate.txt supplies the batch date. Integration steps write alerts and status to DImessages.\nRelated dimension versions must exist before facts are loaded. Holdings also depend on DimTrade.')
    return p


def input_overview(book):
    p = Overview(book,'tpc-di-input-overview','TPC-DI input overview',1160,BLUE)
    c = p.card('customer',40,120,300,200,'Customers',
               'CustomerMgmt.xml supplies\nhistorical customer actions.\nCustomer.txt supplies changes.\nProspect.csv adds demographics.')
    a = p.card('account',420,120,300,200,'Accounts and cash',
               'CustomerMgmt.xml supplies\nhistorical account actions.\nAccount.txt supplies changes.\nCashTransaction.txt moves cash.')
    b = p.card('broker',800,120,300,200,'Employees',
               'HR.csv describes employees.\nAccount broker identifiers\nrefer to HR employee identifiers.')
    w = p.card('watch',40,450,300,200,'Watch actions',
               'WatchHistory.txt records when\na customer starts or stops\nwatching a security.')
    t = p.card('trade',420,450,300,200,'Trades and holdings',
               'Trade.txt supplies trade states.\nTradeHistory.txt supplies\nhistorical status changes.\nHoldingHistory.txt records changes.')
    sec = p.card('security',800,450,300,200,'Securities and prices',
                 'FINWIRE SEC describes securities.\nDailyMarket.txt supplies daily\nprices and trading volume.')
    co = p.card('company',800,800,300,160,'Companies',
                'FINWIRE CMP describes companies.\nFINWIRE FIN reports quarterly\nfinancial results.')
    p.edge(a,None,c,None,ss='left',ts='right',label='Customer\nID',label_offset=(0,-26))
    p.edge(a,None,b,None,label='Broker ID',label_offset=(0,-18))
    p.edge(t,None,a,None,ss='top',ts='bottom',label='Account ID',label_offset=(65,0))
    p.edge(t,None,sec,None,label='Symbol',label_offset=(0,-18))
    p.edge(w,None,c,None,ss='top',ts='bottom',label='Customer ID',label_offset=(-70,0))
    p.edge(w,None,sec,None,ss='bottom',ts='bottom',tp=(.25,1),via_y=720,label='Symbol',label_offset=(0,18))
    p.edge(sec,None,co,None,ss='bottom',ts='top',label='Company name or CIK',label_offset=(0,30))
    p.card('reference',40,800,680,160,'Shared reference files',
           'Date.txt and Time.txt define calendar values.\nIndustry.txt, StatusType.txt, TaxRate.txt, and TradeType.txt\nsupply code descriptions, rates, and flags.')
    p.card('load',40,1010,1060,110,'Load records',
           'BatchDate.txt supplies the batch date. The *_audit.csv files supply generated audit observations.')
    return p


def warehouse_overview(book):
    p = Overview(book,'tpc-di-warehouse-overview','TPC-DI warehouse overview',1150,GREEN)
    c = p.card('customer',40,120,300,200,'Customers and accounts',
               'DimCustomer\nDimAccount\nDimBroker')
    co = p.card('company',800,120,300,200,'Companies and securities',
                'DimCompany\nDimSecurity\nFinancial')
    t = p.card('trade',420,430,300,160,'DimTrade',
               'Each trade has one row.\nThe row holds its state and\noriginal dimension context.')
    cash = p.card('cash',40,430,300,125,'FactCashBalances',
                  'Each row holds an account\nbalance after daily changes.')
    market = p.card('market',800,430,300,125,'FactMarketHistory',
                    'Each row holds market measures\nfor one security and date.')
    prospect = p.card('prospect',40,680,300,125,'Prospect',
                      'Agency prospect attributes\nmatch customers by name\nand address.')
    holding = p.card('holding',420,680,300,125,'FactHoldings',
                     'Each row records a holding\nchange caused by a trade.')
    watch = p.card('watch',800,680,300,125,'FactWatches',
                   'Each row records a customer\nand security watch interval.')
    p.edge(t,None,c,None,ss='left',via_x=370,ts='right',tp=(1,.7))
    p.edge(t,None,co,None,via_x=770,tp=(0,.7))
    p.edge(cash,None,c,None,ss='top',ts='bottom')
    p.edge(market,None,co,None,ss='top',ts='bottom')
    p.edge(prospect,None,c,None,ss='left',ts='left',via_x=16)
    p.edge(holding,None,t,None,ss='top',ts='bottom')
    p.edge(watch,None,co,None,ss='right',ts='right',via_x=1124)
    p.edge(watch,None,c,None,ss='left',ts='bottom',tp=(.8,1),
           points=[(760,742.5),(760,640),(385,640),(385,355),(280,355)])
    p.card('reference',40,880,1060,110,'Shared calendar and code tables',
           'DimDate and DimTime supply date and time keys.\nIndustry, StatusType, TaxRate, and TradeType supply lookup values for transformations.')
    p.card('load',40,1020,1060,90,'Load records',
           'DImessages records integration messages. Audit stores generated audit observations.')
    return p


def build(export=False):
    book = ET.Element('mxfile',host='app.diagrams.net',type='device',version='32.6.1')
    pages = [fn(book) for fn in (data_flow,input_overview,warehouse_overview)]
    write_views(book,pages,OUT/'tpc-di-overviews.drawio',export)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export',action='store_true',help='Export all three overview images.')
    build(parser.parse_args().export)
