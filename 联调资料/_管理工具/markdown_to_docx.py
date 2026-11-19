"""Export a local Markdown report to a styled Word document using existing python-docx."""
import os
from pathlib import Path
import re
import sys
from urllib.parse import quote

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.opc.constants import RELATIONSHIP_TYPE
from docx.shared import Cm, Pt, RGBColor

def render(source,output):
 source,output=Path(source).resolve(),Path(output).resolve()
 doc=Document();section=doc.sections[0]
 section.page_width=Cm(21);section.page_height=Cm(29.7)
 section.top_margin=section.bottom_margin=Cm(1.8)
 section.left_margin=section.right_margin=Cm(1.9)
 for name in ['Normal','List Bullet','List Number']:
  style=doc.styles[name];style.font.name='Calibri';style.font.size=Pt(10.5)
  style.element.get_or_add_rPr().rFonts.set(qn('w:eastAsia'),'宋体')
  style.paragraph_format.line_spacing=1.18;style.paragraph_format.space_after=Pt(5)
 for level,size in [(1,16),(2,13),(3,11.5)]:
  style=doc.styles[f'Heading {level}'];style.font.name='Calibri';style.font.size=Pt(size);style.font.color.rgb=RGBColor.from_string('17365D')
  style.element.get_or_add_rPr().rFonts.set(qn('w:eastAsia'),'微软雅黑')
  style.paragraph_format.space_before=Pt(12);style.paragraph_format.space_after=Pt(6)
  style.paragraph_format.keep_with_next=True
 doc.core_properties.author='项目联调记录'
 doc.core_properties.subject='接口联调过程、范围、验证结果及证据'

 def link(paragraph,label,target):
  if not re.match(r'^[a-z]+://',target,re.I):
   target=os.path.relpath((source.parent/target).resolve(),output.parent).replace('\\','/')
  relation=paragraph.part.relate_to(quote(target,safe='/:-._~#?='),RELATIONSHIP_TYPE.HYPERLINK,is_external=True)
  hyper=OxmlElement('w:hyperlink');hyper.set(qn('r:id'),relation)
  run=OxmlElement('w:r');props=OxmlElement('w:rPr');color=OxmlElement('w:color');color.set(qn('w:val'),'0563C1');props.append(color)
  underline=OxmlElement('w:u');underline.set(qn('w:val'),'single');props.append(underline);run.append(props)
  text=OxmlElement('w:t');text.text=label;run.append(text);hyper.append(run);paragraph._p.append(hyper)
 def inline(paragraph,text):
  tokens=re.split(r'(\[[^\]]+\]\([^\)]+\)|\*\*[^*]+\*\*|`[^`]+`)',text)
  for token in tokens:
   if not token:continue
   m=re.fullmatch(r'\[([^\]]+)\]\(([^\)]+)\)',token)
   if m:link(paragraph,m[1],m[2]);continue
   run=paragraph.add_run(token[2:-2] if token.startswith('**') else token[1:-1] if token.startswith('`') else token)
   if token.startswith('**'):run.bold=True
   if token.startswith('`'):run.font.name='Consolas';run.font.size=Pt(9)
 lines=source.read_text(encoding='utf-8-sig').splitlines();i=0;code=False
 while i<len(lines):
  line=lines[i].strip();i+=1
  if line.startswith('```'):code=not code;continue
  if code:
   p=doc.add_paragraph();run=p.add_run(lines[i-1]);run.font.name='Consolas';run.font.size=Pt(8.5);p.paragraph_format.space_after=Pt(1);continue
  if not line:continue
  if line.startswith('|'):
   rows=[line]
   while i<len(lines) and lines[i].strip().startswith('|'):rows.append(lines[i].strip());i+=1
   parsed=[[c.strip() for c in r.strip('|').split('|')] for r in rows]
   parsed=[r for r in parsed if not all(re.fullmatch(r'[:\- ]+',c or '-') for c in r)]
   count=len(parsed[0]);table=doc.add_table(rows=0,cols=count);table.style='Table Grid';table.alignment=WD_TABLE_ALIGNMENT.CENTER
   widths={2:[3.8,13.4],4:[1.0,2.0,7.2,7.0]}.get(count)
   if widths:
    table.autofit=False
    for column,width in zip(table.columns,widths):column.width=Cm(width)
   for ridx,cells in enumerate(parsed):
    row=table.add_row()
    for j,text in enumerate(cells):
     cell=row.cells[j];cell.vertical_alignment=WD_CELL_VERTICAL_ALIGNMENT.CENTER;inline(cell.paragraphs[0],text)
     if widths:cell.width=Cm(widths[j])
     for run in cell.paragraphs[0].runs:run.font.size=Pt(9)
     if ridx==0:
      shade=OxmlElement('w:shd');shade.set(qn('w:fill'),'E8EFF7');cell._tc.get_or_add_tcPr().append(shade)
      for run in cell.paragraphs[0].runs:run.bold=True
    if ridx==0:
     repeat=OxmlElement('w:tblHeader');row._tr.get_or_add_trPr().append(repeat)
   doc.add_paragraph().paragraph_format.space_after=Pt(1)
   continue
  match=re.match(r'^(#{1,4})\s+(.*)',line)
  if match:
   level=len(match[1]);title=match[2]
   if level==1:
    p=doc.add_paragraph();p.alignment=WD_ALIGN_PARAGRAPH.CENTER
    run=p.add_run(title);run.bold=True;run.font.size=Pt(21);run.font.name='Calibri';run._element.get_or_add_rPr().rFonts.set(qn('w:eastAsia'),'微软雅黑')
    p.paragraph_format.space_after=Pt(15);doc.core_properties.title=title
   else:inline(doc.add_paragraph(style=f'Heading {min(level-1,3)}'),title)
   continue
  bullet=re.match(r'^[-*]\s+(.*)',line);number=re.match(r'^\d+\.\s+(.*)',line)
  p=doc.add_paragraph(style='List Bullet' if bullet else 'List Number' if number else 'Normal')
  inline(p,(bullet or number)[1] if (bullet or number) else line)
 header=section.header.paragraphs[0];header.text='项目联调记录  |  接口、过程与验证证据'
 for run in header.runs:run.font.size=Pt(8);run.font.color.rgb=RGBColor(100,100,100)
 footer=section.footer.paragraphs[0];footer.alignment=WD_ALIGN_PARAGRAPH.RIGHT
 footer.add_run('第 ')
 for field in ['PAGE','NUMPAGES']:
  if field=='NUMPAGES':footer.add_run(' 页 / 共 ')
  item=OxmlElement('w:fldSimple');item.set(qn('w:instr'),field);footer._p.append(item)
 footer.add_run(' 页')
 update=OxmlElement('w:updateFields');update.set(qn('w:val'),'true');doc.settings.element.append(update)
 output.parent.mkdir(parents=True,exist_ok=True);doc.save(output)
 return output

if __name__=='__main__':print(render(sys.argv[1],sys.argv[2]))
