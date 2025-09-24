// public/scripts.js

function exportarExcel() {
  const table = document.getElementById('tablaReporte');
  if (!table) return alert('Tabla no encontrada.');
  const wb = XLSX.utils.table_to_book(table, { sheet: "Cierre Diario" });
  XLSX.writeFile(wb, `cierre_diario_${new Date().toISOString().split('T')[0]}.xlsx`);
}

function exportarPDF() {
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF('p', 'mm', 'a4');
  const element = document.getElementById('tablaReporte');
  if (!element) return alert('Tabla no encontrada.');
  
  html2canvas(element, { scale: 2 }).then(canvas => {
    const imgData = canvas.toDataURL('image/png');
    const imgWidth = 190;
    const pageHeight = 295;
    const imgHeight = (canvas.height * imgWidth) / canvas.width;
    let heightLeft = imgHeight;
    let position = 10;

    doc.addImage(imgData, 'PNG', 10, position, imgWidth, imgHeight);
    heightLeft -= pageHeight;

    while (heightLeft > 0) {
      doc.addPage();
      doc.addImage(imgData, 'PNG', 10, 10, imgWidth, imgHeight);
      heightLeft -= pageHeight;
    }
    doc.save(`cierre_diario_${new Date().toISOString().split('T')[0]}.pdf`);
  });
}

function imprimir() {
  window.print();
}