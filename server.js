// server.js
// Sistema Integrado: Inventario + Reportes Comerciales + Cierre de Caja - SAP B1 HANA
const express = require('express');
const hana = require('@sap/hana-client');
const fs = require('fs').promises;
const path = require('path');
const app = express();
const PORT = 3001;

// === CONFIGURACIÓN DE CONEXIÓN A SAP B1 HANA ===
const connOptions = {
  serverNode: '162.248.53.60:30015',
  uid: 'SYSTEM',
  pwd: 'B1Admin$',
  connectTimeout: 120000,
  statementTimeout: 120000
};

// === CONFIGURACIÓN DE CACHÉ PARA INVENTARIO ===
let inventarioCache = null;
let ultimaActualizacion = null;
const CACHE_FILE = path.join(__dirname, 'inventario_cache.json');

/**
 * Función para cargar inventario desde HANA (solo se llama manualmente)
 */
async function cargarInventarioManual() {
  return new Promise((resolve, reject) => {
    console.log('🔄 Cargando inventario desde HANA (manual)...');
    const conn = hana.createConnection();
    const timeout = setTimeout(() => {
      conn.disconnect();
      reject(new Error('Timeout: La consulta tardó más de 120 segundos.'));
    }, 120000);

    conn.connect(connOptions, (err) => {
      if (err) {
        clearTimeout(timeout);
        return reject(err);
      }

      const query = `
        SELECT 
          T0."ItemCode",
          T1."ItemName",
          (SELECT "Price" FROM "SBO_HVGA_PROD"."ITM1" WHERE "ItemCode" = T1."ItemCode" AND "PriceList" = 1) AS "PrecioVenta",
          T0."WhsCode",
          T2."WhsName",
          T0."OnHand",
          T0."IsCommited",
          (T0."OnHand" - IFNULL(T0."IsCommited", 0)) AS "DisponibleFinal",
          Transito."Transito_Total",
          Transito."Transito_Disponible"
        FROM "SBO_HVGA_PROD"."DISPONIBLE_LOCAL_BODEGA_STOCK_V1"('', '') T0
        JOIN "SBO_HVGA_PROD"."OITM" T1 ON T0."ItemCode" = T1."ItemCode"
        JOIN "SBO_HVGA_PROD"."OWHS" T2 ON T0."WhsCode" = T2."WhsCode"
        LEFT JOIN (
          SELECT 
            "Articulo",
            SUM("Cantidad") AS "Transito_Total",
            SUM("Disponible") AS "Transito_Disponible"
          FROM "SBO_HVGA_PROD"."DISPONIBLE_IMPORTADO_LOTES"()
          GROUP BY "Articulo"
        ) Transito ON T0."ItemCode" = Transito."Articulo"
        WHERE (T0."OnHand" - IFNULL(T0."IsCommited", 0)) >= 0 OR IFNULL(Transito."Transito_Disponible", 0) > 0
        ORDER BY T1."ItemName"
        LIMIT 1000
      `;

      conn.exec(query, (err, rows) => {
        clearTimeout(timeout);
        conn.disconnect();
        if (err) {
          return reject(err);
        }

        // Agrupar productos por ItemCode
        const productos = {};
        rows.forEach(row => {
          const code = row.ItemCode;
          if (!productos[code]) {
            productos[code] = {
              ItemCode: code,
              ItemName: row.ItemName,
              PrecioVenta: row.PrecioVenta || null,
              bodegas: [],
              Transito_Total: row.Transito_Total || 0,
              Transito_Disponible: row.Transito_Disponible || 0
            };
          }
          productos[code].bodegas.push({
            WhsCode: row.WhsCode,
            WhsName: row.WhsName,
            OnHand: row.OnHand,
            IsCommited: row.IsCommited,
            DisponibleFinal: row.DisponibleFinal
          });
        });

        const data = Object.values(productos);
        const fecha = new Date();
        inventarioCache = data;
        ultimaActualizacion = fecha;

        guardarCacheEnArchivo(data, fecha)
          .then(() => {
            console.log(`✅ Inventario cargado manualmente: ${data.length} productos`);
            resolve();
          })
          .catch(reject);
      });
    });
  });
}

/**
 * Función para guardar caché en archivo
 */
async function guardarCacheEnArchivo(data, fecha) {
  try {
    const cacheData = {
      productos: data,
      ultimaActualizacion: fecha
    };
    await fs.writeFile(CACHE_FILE, JSON.stringify(cacheData, null, 2), 'utf8');
    console.log(`💾 Caché guardado en archivo: ${CACHE_FILE}`);
  } catch (error) {
    console.error('❌ Error al guardar caché en archivo:', error.message);
    throw error;
  }
}

/**
 * Función para cargar caché desde archivo
 */
async function cargarCacheDesdeArchivo() {
  try {
    const stats = await fs.stat(CACHE_FILE).catch(() => null);
    if (stats) {
      const rawData = await fs.readFile(CACHE_FILE, 'utf8');
      const cacheData = JSON.parse(rawData);
      inventarioCache = cacheData.productos;
      ultimaActualizacion = new Date(cacheData.ultimaActualizacion);
      console.log(`📂 Caché cargado desde archivo: ${inventarioCache.length} productos - ${ultimaActualizacion.toLocaleString()}`);
    }
  } catch (error) {
    console.error('❌ Error al cargar caché desde archivo:', error.message);
  }
}

// Middleware
app.use(express.json());

// ===================================================================
// 🔹 Middleware para eliminar la página de advertencia de ngrok
// ===================================================================
app.use((req, res, next) => {
  res.setHeader('ngrok-skip-browser-warning', 'true');
  next();
});

// === CONFIGURACIÓN PARA EJS ===
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));

// === RUTA RAÍZ: Página principal con enlaces ===
app.get('/', (req, res) => {
  res.render('index');
});

// ===================================================================
// 🔹 RUTA: /inventario - VERSIÓN ULTRA SEGURA (NO se cuelga nunca)
// ===================================================================
app.get('/inventario', async (req, res) => {
  if (!inventarioCache) {
    await cargarCacheDesdeArchivo();
  }
  if (inventarioCache) {
    res.render('inventario', {
      productos: inventarioCache,
      ultimaActualizacion: ultimaActualizacion
    });
    return;
  }
  // Si no hay caché, mostrar pantalla de carga
  res.render('inventario_vacio');
});

// Nueva ruta para cargar inventario manualmente
app.get('/cargar-inventario-manual', async (req, res) => {
  try {
    await cargarInventarioManual();
    res.json({ success: true, message: 'Inventario cargado correctamente' });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ===================================================================
// 🔹 RUTA: /reporte-diario - TOTALMENTE CORREGIDO
// ===================================================================
app.get('/reporte-diario', async (req, res) => {
  const {
    fechaInicio,
    fechaFin,
    tiendas,
    asesores,
    cumpleCondicion
  } = req.query;

  const defaultFechaFin = new Date().toISOString().split('T')[0];
  const defaultFechaInicio = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  const fi = fechaInicio || defaultFechaInicio;
  const ff = fechaFin || defaultFechaFin;

  // Manejo seguro de parámetros de filtro
  const tList = tiendas ? (Array.isArray(tiendas) ? tiendas : [tiendas]) : [];
  const aList = asesores ? (Array.isArray(asesores) ? asesores : [asesores]) : [];
  const cumple = cumpleCondicion || '';

  let conn;
  try {
    conn = hana.createConnection();
    await new Promise((resolve, reject) => {
      conn.connect(connOptions, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    // Consultas para llenar los filtros (solo tiendas y asesores activos, excluyendo autoconsumo/liquidación)
    const [tiendasRows, asesoresRows] = await Promise.all([
      new Promise((resolve, reject) => {
        const query = `
          SELECT DISTINCT NNM1."BeginStr", NNM1."SeriesName"
          FROM "SBO_HVGA_PROD"."NNM1"
          WHERE NNM1."ObjectCode" = '17' 
            AND NNM1."SeriesName" IN ('BOSQUE', 'TUMBACO', 'IBARRA', 'PLAZA', 'CUENCA', 'LOJA')
          ORDER BY 
            CASE NNM1."SeriesName"
              WHEN 'BOSQUE' THEN 1
              WHEN 'TUMBACO' THEN 2
              WHEN 'IBARRA' THEN 3
              WHEN 'PLAZA' THEN 4
              WHEN 'CUENCA' THEN 5
              WHEN 'LOJA' THEN 6
              ELSE 99
            END
        `;
        conn.exec(query, (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        });
      }),
      new Promise((resolve, reject) => {
        const query = `
          SELECT DISTINCT "SlpCode", "SlpName"
          FROM "SBO_HVGA_PROD"."OSLP"
          WHERE "SlpName" NOT LIKE '%AUTOCONSUMO%' 
            AND "SlpName" NOT LIKE '%LIQUIDACION%'
            AND "Active" = 'Y'
          ORDER BY "SlpName"
        `;
        conn.exec(query, (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        });
      })
    ]);

    // Construir cláusula WHERE dinámica
    let whereClause = `
      ORDR."CANCELED" = 'N'
      AND ORDR."DocDate" BETWEEN '${fi}' AND '${ff}'
      AND SLP."SlpName" NOT LIKE '%AUTOCONSUMO%'
      AND SLP."SlpName" NOT LIKE '%LIQUIDACION%'
      AND SLP."Active" = 'Y'
    `;

    if (tList.length > 0) {
      const tCond = tList.map(t => `'${t}'`).join(',');
      whereClause += ` AND NNM1."BeginStr" IN (${tCond})`;
    }

    if (aList.length > 0) {
      const aCond = aList.join(',');
      whereClause += ` AND ORDR."SlpCode" IN (${aCond})`;
    }

    if (cumple === 'SI' || cumple === 'NO') {
      const condition = cumple === 'SI' ? 'SI' : 'NO';
      whereClause += ` AND (
        CASE 
          WHEN (ORDR."U_HV_NecesCred" IN ('SI', 'Y', '1', '01'))
               AND (ORDR."U_HV_CredApro" IN ('1', '01')) THEN 'SI'
          WHEN ORDR."DocStatus" = 'C' THEN 'SI'
          WHEN (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
               WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) > 0
               AND ORDR."U_HV_CredApro" IN ('1', '01')
               AND ORDR."U_HV_TipoPago" IN ('3','5','6','7','8','9')
               AND (SELECT COALESCE(SUM("DocTotal"), 0) / NULLIF(ORDR."DocTotal", 0) * 100
                    FROM "SBO_HVGA_PROD"."ORCT"
                    WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) >= 30
               AND ORDR."DocTotal" > 0 THEN 'SI'
          WHEN COALESCE(ORDR."U_HV_CredApro", '') = 'NA'
               AND ORDR."U_HV_CredApro" NOT IN ('1', '01')
               AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                    WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) = 0
               AND ORDR."DocTotal" > 0
               AND (SELECT COALESCE(SUM(OINV."PaidToDate") / SUM(OINV."DocTotal"), 0) * 100
                    FROM "SBO_HVGA_PROD"."OINV" OINV
                    INNER JOIN "SBO_HVGA_PROD"."INV1" ON OINV."DocEntry" = INV1."DocEntry"
                    WHERE INV1."BaseEntry" = ORDR."DocEntry" 
                      AND INV1."BaseType" = 17
                      AND OINV."CANCELED" = 'N') >= 30 THEN 'SI'
          WHEN COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
               AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                    WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) > 0
               AND (SELECT COALESCE(SUM("DocTotal"), 0) / NULLIF(ORDR."DocTotal", 0) * 100
                    FROM "SBO_HVGA_PROD"."ORCT"
                    WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) >= 30
               AND ORDR."DocTotal" > 0 THEN 'SI'
          WHEN COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
               AND ORDR."U_HV_CredApro" IN ('1', '01')
               AND ORDR."U_HV_TipoPago" IN ('3','5','6','7','8','9')
               AND ORDR."DocTotal" > 0 THEN 'SI'
          WHEN COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
               AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                    WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) = 0
               AND ORDR."DocTotal" > 0
               AND (SELECT COALESCE(SUM(OINV."PaidToDate") / SUM(OINV."DocTotal"), 0) * 100
                    FROM "SBO_HVGA_PROD"."OINV" OINV
                    INNER JOIN "SBO_HVGA_PROD"."INV1" ON OINV."DocEntry" = INV1."DocEntry"
                    WHERE INV1."BaseEntry" = ORDR."DocEntry" 
                      AND INV1."BaseType" = 17
                      AND OINV."CANCELED" = 'N') >= 50 THEN 'SI'
          WHEN ORDR."U_HV_Autoriza" = 'SI' THEN 'SI'
          ELSE 'NO'
        END = '${condition}'
      )`;
    }

    // Consulta principal
    const query = `
      SELECT 
        NNM1."SeriesName" AS "Tienda",
        'Pedido' AS "Tipo",
        ORDR."CardName" AS "Nombre de cliente",
        CAST(ORDR."DocNum" AS INTEGER) AS "N° pedido",
        RDR1."ItemCode" AS "Código del Producto",
        RDR1."Dscription" AS "Descripción Articulo",
        RDR1."Quantity" AS "Cantidad",
        RDR1."DiscPrcnt" AS "% Descuento",
        RDR1."LineTotal" AS "Valor Venta",
        ORDR."DocTotal" AS "Valor Pedido (Cabecera)",
        SLP."SlpName" AS "Asesor",
        CASE ORDR."U_HV_TipoPago"
            WHEN '1' THEN 'Contado'
            WHEN '2' THEN 'Debito'
            WHEN '3' THEN 'Credito'
            WHEN '4' THEN 'Corriente'
            WHEN '5' THEN 'Planes con Int'
            WHEN '6' THEN '3 meses sin Int'
            WHEN '7' THEN '6 meses sin Int'
            WHEN '8' THEN '9 meses sin Int'
            WHEN '9' THEN '12 meses sin Int'
            WHEN '10' THEN 'Tarjeta de Regalo'
            ELSE 'Otros'
        END AS "Tipo Pago",
        CASE 
            WHEN (ORDR."U_HV_NecesCred" IN ('SI', 'Y', '1', '01'))
                 AND (ORDR."U_HV_CredApro" IN ('1', '01')) THEN 'SI'
            WHEN ORDR."DocStatus" = 'C' THEN 'SI'
            WHEN (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                 WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) > 0
                 AND ORDR."U_HV_CredApro" IN ('1', '01')
                 AND ORDR."U_HV_TipoPago" IN ('3','5','6','7','8','9')
                 AND (SELECT COALESCE(SUM("DocTotal"), 0) / NULLIF(ORDR."DocTotal", 0) * 100
                      FROM "SBO_HVGA_PROD"."ORCT"
                      WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) >= 30
                 AND ORDR."DocTotal" > 0 THEN 'SI'
            WHEN COALESCE(ORDR."U_HV_CredApro", '') = 'NA'
                 AND ORDR."U_HV_CredApro" NOT IN ('1', '01')
                 AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                      WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) = 0
                 AND ORDR."DocTotal" > 0
                 AND (SELECT COALESCE(SUM(OINV."PaidToDate") / SUM(OINV."DocTotal"), 0) * 100
                      FROM "SBO_HVGA_PROD"."OINV" OINV
                      INNER JOIN "SBO_HVGA_PROD"."INV1" ON OINV."DocEntry" = INV1."DocEntry"
                      WHERE INV1."BaseEntry" = ORDR."DocEntry" 
                        AND INV1."BaseType" = 17
                        AND OINV."CANCELED" = 'N') >= 30 THEN 'SI'
            WHEN COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
                 AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                      WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) > 0
                 AND (SELECT COALESCE(SUM("DocTotal"), 0) / NULLIF(ORDR."DocTotal", 0) * 100
                      FROM "SBO_HVGA_PROD"."ORCT"
                      WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) >= 30
                 AND ORDR."DocTotal" > 0 THEN 'SI'
            WHEN COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
                 AND ORDR."U_HV_CredApro" IN ('1', '01')
                 AND ORDR."U_HV_TipoPago" IN ('3','5','6','7','8','9')
                 AND ORDR."DocTotal" > 0 THEN 'SI'
            WHEN COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
                 AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                      WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) = 0
                 AND ORDR."DocTotal" > 0
                 AND (SELECT COALESCE(SUM(OINV."PaidToDate") / SUM(OINV."DocTotal"), 0) * 100
                      FROM "SBO_HVGA_PROD"."OINV" OINV
                      INNER JOIN "SBO_HVGA_PROD"."INV1" ON OINV."DocEntry" = INV1."DocEntry"
                      WHERE INV1."BaseEntry" = ORDR."DocEntry" 
                        AND INV1."BaseType" = 17
                        AND OINV."CANCELED" = 'N') >= 50 THEN 'SI'
            WHEN ORDR."U_HV_Autoriza" = 'SI' THEN 'SI'
            ELSE 'NO'
        END AS "CumpleCondicion"
      FROM "SBO_HVGA_PROD"."ORDR"
      INNER JOIN "SBO_HVGA_PROD"."RDR1" ON ORDR."DocEntry" = RDR1."DocEntry"
      INNER JOIN "SBO_HVGA_PROD"."OSLP" SLP ON ORDR."SlpCode" = SLP."SlpCode"
      INNER JOIN "SBO_HVGA_PROD"."NNM1" NNM1 ON ORDR."Series" = NNM1."Series" AND NNM1."ObjectCode" = '17'
      WHERE ${whereClause}
      UNION ALL
      SELECT 
        NNM1."SeriesName" AS "Tienda",
        'Ajuste_NC' AS "Tipo",
        SLP."SlpName" AS "Nombre de cliente",
        0 AS "N° pedido",
        '' AS "Código del Producto",
        'AJUSTE POR DEVOLUCIONES REALIZADAS EN EL PERIODO' AS "Descripción Articulo",
        0 AS "Cantidad",
        0 AS "% Descuento",
        -SUM(ORIN."DocTotal") AS "Valor Venta",
        NULL AS "Valor Pedido (Cabecera)",
        SLP."SlpName" AS "Asesor",
        'N/A' AS "Tipo Pago",
        'SI' AS "CumpleCondicion"
      FROM "SBO_HVGA_PROD"."ORIN" ORIN
      INNER JOIN "SBO_HVGA_PROD"."OSLP" SLP ON ORIN."SlpCode" = SLP."SlpCode"
      INNER JOIN "SBO_HVGA_PROD"."NNM1" NNM1 ON ORIN."Series" = NNM1."Series" AND NNM1."ObjectCode" = '14'
      WHERE 
        ORIN."CANCELED" = 'N'
        AND ORIN."DocDate" BETWEEN '${fi}' AND '${ff}'
        AND SLP."SlpName" NOT LIKE '%AUTOCONSUMO%'
        AND SLP."SlpName" NOT LIKE '%LIQUIDACION%'
        AND SLP."Active" = 'Y'
        ${tList.length > 0 ? `AND NNM1."BeginStr" IN (${tList.map(t => `'${t}'`).join(',')})` : ''}
      GROUP BY 
        SLP."SlpName", NNM1."SeriesName"
      ORDER BY 
        "Tienda", 
        "Asesor", 
        "Tipo", 
        "N° pedido"
    `;

    const rows = await new Promise((resolve, reject) => {
      conn.exec(query, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    // Calcular estadísticas
    const clientesUnicos = [...new Set(rows.map(r => r["Nombre de cliente"]))].length;
    const promedioDescuento = rows.length > 0 ? rows.reduce((sum, r) => sum + parseFloat(r["% Descuento"] || 0), 0) / rows.length : 0;
    const totalValor = rows.reduce((sum, r) => sum + parseFloat(r["Valor Venta"] || 0), 0);

    // Calcular totales para el pie de tabla
    const totalCantidad = rows.reduce((sum, r) => sum + parseFloat(r["Cantidad"] || 0), 0);
    const totalValorVenta = rows.reduce((sum, r) => sum + parseFloat(r["Valor Venta"] || 0), 0);
    const totalValorPedido = rows.reduce((sum, r) => sum + parseFloat(r["Valor Pedido (Cabecera)"] || 0), 0);

    // Renderizar la vista EJS
    res.render('reporte_diario', {
      fi,
      ff,
      tiendasRows,
      asesoresRows,
      tList,
      aList,
      cumple,
      rows,
      clientesUnicos,
      promedioDescuento,
      totalValor,
      totalCantidad,
      totalValorVenta,
      totalValorPedido
    });
  } catch (error) {
    console.error('Error en /reporte-diario:', error);
    res.status(500).render('error', { message: error.message });
  } finally {
    if (conn) {
      conn.disconnect();
    }
  }
});

// ===================================================================
// 🔹 RUTA: /reporte-acumulado - USANDO EJS Y LA CONSULTA SQL AVANZADA
// ===================================================================
app.get('/reporte-acumulado', async (req, res) => {
  const {
    fechaInicio,
    fechaFin,
    sucursales,
    vendedores
  } = req.query;

  const defaultFechaFin = new Date().toISOString().split('T')[0];
  const defaultFechaInicio = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  const fi = fechaInicio || defaultFechaInicio;
  const ff = fechaFin || defaultFechaFin;

  const sucList = sucursales ? (Array.isArray(sucursales) ? sucursales : [sucursales]) : [];
  const vendList = vendedores ? (Array.isArray(vendedores) ? vendedores : [vendedores]) : [];

  let conn;
  try {
    conn = hana.createConnection();
    await new Promise((resolve, reject) => {
      conn.connect(connOptions, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    // Consultas para llenar los filtros (EXCLUYENDO Autoconsumo y Liquidación)
    const [sucursalesRows, vendedoresRows] = await Promise.all([
      new Promise((resolve, reject) => {
        const query = `
          SELECT DISTINCT NNM1."BeginStr", NNM1."SeriesName"
          FROM "SBO_HVGA_PROD"."NNM1"
          WHERE NNM1."ObjectCode" = '17' 
            AND NNM1."SeriesName" IN ('BOSQUE', 'TUMBACO', 'IBARRA', 'PLAZA', 'CUENCA', 'LOJA')
          ORDER BY 
            CASE NNM1."SeriesName"
              WHEN 'BOSQUE' THEN 1
              WHEN 'TUMBACO' THEN 2
              WHEN 'IBARRA' THEN 3
              WHEN 'PLAZA' THEN 4
              WHEN 'CUENCA' THEN 5
              WHEN 'LOJA' THEN 6
              ELSE 99
            END
        `;
        conn.exec(query, (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        });
      }),
      new Promise((resolve, reject) => {
        const query = `
          SELECT DISTINCT "SlpCode", "SlpName"
          FROM "SBO_HVGA_PROD"."OSLP"
          WHERE "SlpName" NOT LIKE '%AUTOCONSUMO%' 
            AND "SlpName" NOT LIKE '%LIQUIDACION%'
            AND "Active" = 'Y'
          ORDER BY "SlpName"
        `;
        conn.exec(query, (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        });
      })
    ]);

    // Construir cláusula WHERE (EXCLUYENDO Autoconsumo y Liquidación)
    let whereClause = `
      ORDR."CANCELED" = 'N'
      AND ORDR."DocDate" BETWEEN '${fi}' AND '${ff}'
      AND SLP."SlpName" NOT LIKE '%AUTOCONSUMO%'
      AND SLP."SlpName" NOT LIKE '%LIQUIDACION%'
      AND SLP."Active" = 'Y'
    `;

    if (sucList.length > 0) {
      const sucCond = sucList.map(s => `'${s}'`).join(',');
      whereClause += ` AND NNM1."BeginStr" IN (${sucCond})`;
    }

    if (vendList.length > 0) {
      const vendCond = vendList.join(',');
      whereClause += ` AND ORDR."SlpCode" IN (${vendCond})`;
    }

    // 🚨 ¡CONSULTA SQL AVANZADA FINAL! 🚨
    const query = `
      SELECT 
          NNM1."SeriesName" AS "SUCURSAL",
          SLP."SlpName" AS "Asesor",
          -- VALOR OFERTAS: cotizaciones del periodo
          COALESCE(
              (SELECT SUM(OQUT."DocTotal")
               FROM "SBO_HVGA_PROD"."OQUT" OQUT
               WHERE OQUT."SlpCode" = ORDR."SlpCode"
                 AND OQUT."DocDate" >= '${fi}' 
                 AND OQUT."DocDate" <= '${ff}'
                 AND OQUT."CANCELED" = 'N'
              ), 0
          ) AS "VALOR OFERTAS",
          -- Valor total de todos los pedidos no cancelados
          COALESCE(
              (SELECT SUM(ORDR2."DocTotal")
               FROM "SBO_HVGA_PROD"."ORDR" ORDR2
               WHERE ORDR2."SlpCode" = ORDR."SlpCode"
                 AND ORDR2."DocDate" >= '${fi}' 
                 AND ORDR2."DocDate" <= '${ff}'
                 AND ORDR2."CANCELED" = 'N'
              ), 0
          ) AS "Valor Pedidos (No Cancelados)",
          -- Valor de pedidos que cumplen con política (CumpleCondicion = 'SI')
          COALESCE(SUM(
              CASE 
                  WHEN (ORDR."U_HV_NecesCred" IN ('SI', 'Y', '1', '01'))
                       AND (ORDR."U_HV_CredApro" IN ('1', '01')) THEN ORDR."DocTotal"
                  WHEN ORDR."DocStatus" = 'C' THEN ORDR."DocTotal"
                  WHEN (
                          (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                           WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) > 0
                          AND ORDR."U_HV_CredApro" IN ('1', '01')
                          AND ORDR."U_HV_TipoPago" IN ('3','5','6','7','8','9')
                          AND (
                              SELECT COALESCE(SUM("DocTotal"), 0) / NULLIF(ORDR."DocTotal", 0) * 100
                              FROM "SBO_HVGA_PROD"."ORCT"
                              WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))
                          ) >= 30
                          AND ORDR."DocTotal" > 0
                      ) THEN ORDR."DocTotal"
                  WHEN (
                          COALESCE(ORDR."U_HV_CredApro", '') = 'NA'
                          AND ORDR."U_HV_CredApro" NOT IN ('1', '01')
                          AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                               WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) = 0
                          AND ORDR."DocTotal" > 0
                          AND (
                              SELECT COALESCE(SUM(OINV."PaidToDate") / SUM(OINV."DocTotal"), 0) * 100
                              FROM "SBO_HVGA_PROD"."OINV" OINV
                              INNER JOIN "SBO_HVGA_PROD"."INV1" ON OINV."DocEntry" = INV1."DocEntry"
                              WHERE INV1."BaseEntry" = ORDR."DocEntry" 
                                AND INV1."BaseType" = 17
                                AND OINV."CANCELED" = 'N'
                          ) >= 30
                      ) THEN ORDR."DocTotal"
                  WHEN (
                          COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
                          AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                               WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) > 0
                          AND (
                              SELECT COALESCE(SUM("DocTotal"), 0) / NULLIF(ORDR."DocTotal", 0) * 100
                              FROM "SBO_HVGA_PROD"."ORCT"
                              WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15)))
                          >= 30
                          AND ORDR."DocTotal" > 0
                      ) THEN ORDR."DocTotal"
                  WHEN (
                          COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
                          AND ORDR."U_HV_CredApro" IN ('1', '01')
                          AND ORDR."U_HV_TipoPago" IN ('3','5','6','7','8','9')
                          AND ORDR."DocTotal" > 0
                      ) THEN ORDR."DocTotal"
                  WHEN (
                          COALESCE(ORDR."U_HV_Autoriza", 'NO') = 'NO'
                          AND (SELECT COUNT(*) FROM "SBO_HVGA_PROD"."ORCT" 
                               WHERE CAST("U_HV_NroOrden" AS NVARCHAR(15)) = CAST(ORDR."DocNum" AS NVARCHAR(15))) = 0
                          AND ORDR."DocTotal" > 0
                          AND (
                              SELECT COALESCE(SUM(OINV."PaidToDate") / SUM(OINV."DocTotal"), 0) * 100
                              FROM "SBO_HVGA_PROD"."OINV" OINV
                              INNER JOIN "SBO_HVGA_PROD"."INV1" ON OINV."DocEntry" = INV1."DocEntry"
                              WHERE INV1."BaseEntry" = ORDR."DocEntry" 
                                AND INV1."BaseType" = 17
                                AND OINV."CANCELED" = 'N'
                          ) >= 50
                      ) THEN ORDR."DocTotal"
                  WHEN ORDR."U_HV_Autoriza" = 'SI' THEN ORDR."DocTotal"
                  ELSE 0
              END
          ), 0) AS "Valor Pedidos Confirmados",
          -- VALOR FACTURACIÓN (bruta)
          COALESCE(
              SUM(
                  (SELECT SUM(UniqueInvoices."DocTotal")
                   FROM (
                       SELECT DISTINCT OINV."DocEntry", OINV."DocTotal"
                       FROM "SBO_HVGA_PROD"."OINV" OINV
                       INNER JOIN "SBO_HVGA_PROD"."INV1" INV1 ON OINV."DocEntry" = INV1."DocEntry"
                       WHERE INV1."BaseEntry" = ORDR."DocEntry"
                         AND INV1."BaseType" = 17
                         AND OINV."CANCELED" = 'N'
                         AND OINV."DocDate" >= '${fi}' 
                         AND OINV."DocDate" <= '${ff}'
                   ) AS UniqueInvoices
                  )
              ), 0
          ) AS "VALOR FACTURACIÓN",
          -- VALOR FACTURACIÓN - NOTAS DE CREDITO (neto)
          COALESCE(
              SUM(
                  (SELECT SUM(UniqueInvoices."DocTotal")
                   FROM (
                       SELECT DISTINCT OINV."DocEntry", OINV."DocTotal"
                       FROM "SBO_HVGA_PROD"."OINV" OINV
                       INNER JOIN "SBO_HVGA_PROD"."INV1" INV1 ON OINV."DocEntry" = INV1."DocEntry"
                       WHERE INV1."BaseEntry" = ORDR."DocEntry"
                         AND INV1."BaseType" = 17
                         AND OINV."CANCELED" = 'N'
                         AND OINV."DocDate" >= '${fi}' 
                         AND OINV."DocDate" <= '${ff}'
                   ) AS UniqueInvoices
                  )
              ), 0
          ) - COALESCE(
              (SELECT SUM(ORIN."DocTotal")
               FROM "SBO_HVGA_PROD"."ORIN" ORIN
               WHERE ORIN."SlpCode" = ORDR."SlpCode"
                 AND ORIN."DocDate" >= '${fi}' 
                 AND ORIN."DocDate" <= '${ff}'
                 AND ORIN."CANCELED" = 'N'
              ), 0
          ) AS "VALOR FACTURACIÓN - NOTAS DE CREDITO",
          -- VALOR NOTAS DE CREDITO
          COALESCE(
              (SELECT SUM(ORIN."DocTotal")
               FROM "SBO_HVGA_PROD"."ORIN" ORIN
               WHERE ORIN."SlpCode" = ORDR."SlpCode"
                 AND ORIN."DocDate" >= '${fi}' 
                 AND ORIN."DocDate" <= '${ff}'
                 AND ORIN."CANCELED" = 'N'
              ), 0
          ) AS "VALOR NOTAS DE CREDITO",
          -- Cobro por Asesor
          COALESCE(
              (SELECT SUM(ORCT."DocTotal")
               FROM "SBO_HVGA_PROD"."ORCT" ORCT
               INNER JOIN "SBO_HVGA_PROD"."ORDR" ORDR3 ON CAST(ORDR3."DocNum" AS NVARCHAR(15)) = CAST(ORCT."U_HV_NroOrden" AS NVARCHAR(15))
               WHERE ORDR3."SlpCode" = ORDR."SlpCode"
                 AND ORCT."DocDate" >= '${fi}' 
                 AND ORCT."DocDate" <= '${ff}'
                 AND ORCT."Canceled" = 'N'
              ), 0
          ) AS "Cobro por Asesor"
      FROM "SBO_HVGA_PROD"."ORDR" ORDR
      INNER JOIN "SBO_HVGA_PROD"."OSLP" SLP ON ORDR."SlpCode" = SLP."SlpCode"
      INNER JOIN "SBO_HVGA_PROD"."NNM1" NNM1 ON ORDR."Series" = NNM1."Series" AND NNM1."ObjectCode" = '17'
      WHERE ${whereClause}
      GROUP BY 
          NNM1."SeriesName",
          SLP."SlpName",
          ORDR."SlpCode"
      ORDER BY 
          "SUCURSAL", 
          "Asesor"
    `;

    const rows = await new Promise((resolve, reject) => {
      conn.exec(query, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    // Calcular totales generales
    let totalOfertas = 0;
    let totalPedidosNoCancelados = 0;
    let totalPedidosConfirmados = 0;
    let totalFacturacion = 0;
    let totalFacturacionNotasCredito = 0;
    let totalNotasCredito = 0;
    let totalCobroAsesor = 0;

    if (rows && rows.length > 0) {
      totalOfertas = rows.reduce((sum, row) => sum + parseFloat(row["VALOR OFERTAS"] || 0), 0);
      totalPedidosNoCancelados = rows.reduce((sum, row) => sum + parseFloat(row["Valor Pedidos (No Cancelados)"] || 0), 0);
      totalPedidosConfirmados = rows.reduce((sum, row) => sum + parseFloat(row["Valor Pedidos Confirmados"] || 0), 0);
      totalFacturacion = rows.reduce((sum, row) => sum + parseFloat(row["VALOR FACTURACIÓN"] || 0), 0);
      totalFacturacionNotasCredito = rows.reduce((sum, row) => sum + parseFloat(row["VALOR FACTURACIÓN - NOTAS DE CREDITO"] || 0), 0);
      totalNotasCredito = rows.reduce((sum, row) => sum + parseFloat(row["VALOR NOTAS DE CREDITO"] || 0), 0);
      totalCobroAsesor = rows.reduce((sum, row) => sum + parseFloat(row["Cobro por Asesor"] || 0), 0);
    }

    // Renderizar la vista EJS
    res.render('reporte_acumulado', {
      fi,
      ff,
      sucursalesRows,
      vendedoresRows,
      sucList,
      vendList,
      rows,
      totalOfertas,
      totalPedidosNoCancelados,
      totalPedidosConfirmados,
      totalFacturacion,
      totalFacturacionNotasCredito,
      totalNotasCredito,
      totalCobroAsesor
    });
  } catch (error) {
    console.error('Error en /reporte-acumulado:', error);
    res.status(500).render('error', { message: error.message });
  } finally {
    if (conn) {
      conn.disconnect();
    }
  }
});

// ===================================================================
// 🔹 RUTA: /cierre-diario - CORREGIDA: SIN DO BEGIN, CON FILTROS Y SIN ERRORES
// ===================================================================
app.get('/cierre-diario', async (req, res) => {
  const { fecha, tiendas } = req.query;
  const fechaSeleccionada = fecha || new Date().toISOString().split('T')[0];
  const tList = tiendas ? (Array.isArray(tiendas) ? tiendas : [tiendas]) : [];
  let conn;
  try {
    conn = hana.createConnection();
    await new Promise((resolve, reject) => {
      conn.connect(connOptions, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    // ✅ Consulta SQL SIN DO BEGIN, solo SELECT + UNION ALL
    const query = `
      --Cheques 	
      SELECT  'CHEQUE' AS "Tipo"
        , (CASE WHEN T8."DocNum" IS NOT NULL THEN 'COBROS' 
               WHEN T9."DocNum" IS NOT NULL THEN 'COBROS ANTICIPOS PEDIDOS'
               WHEN T8."DocNum" IS NULL AND T9."DocNum" IS NULL THEN 'PAGO A CUENTA' END) AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", IFNULL(T9."DocNum",T12."TransId")) AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , (CASE WHEN T7."SumApplied" IS NULL THEN T1."CheckSum"
            ELSE ROUND((CASE WHEN (T8."DocNum" IS NULL AND T9."DocNum" IS NULL AND T12."TransId" IS NULL) THEN T0."CheckSum"
                    ELSE (T7."SumApplied"*T1."CheckSum")/T0."DocTotal" END), 2) 
            END) AS "Importe"
        , T4."BankName" AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."RCT1" T1 ON T0."DocEntry" = T1."DocNum" 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."ODSC" T4 ON T1."BankCode" = T4."BankCode"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."OJDT" T12 ON T12."TransId" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."CheckSum" > 0

      UNION ALL

      --Efectivo 
      SELECT  'EFECTIVO' AS "Tipo"
        , (CASE WHEN T8."DocNum" IS NOT NULL THEN 'COBROS' 
               WHEN T9."DocNum" IS NOT NULL THEN 'COBROS ANTICIPOS PEDIDOS'
               WHEN T8."DocNum" IS NULL AND T9."DocNum" IS NULL THEN 'PAGO A CUENTA' END) AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", IFNULL(T9."DocNum",T12."TransId")) AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , ROUND((CASE WHEN (T8."DocNum" IS NULL AND T9."DocNum" IS NULL AND T12."TransId" IS NULL) THEN T0."CashSum"
                ELSE (T7."SumApplied"*T0."CashSum")/T0."DocTotal" END),2) AS "Importe"
        , '' AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."OJDT" T12 ON T12."TransId" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."CashSum" > 0

      UNION ALL

      --Transferencia 
      SELECT  'TRANSFERENCIA' AS "Tipo"
        , (CASE WHEN T8."DocNum" IS NOT NULL THEN 'COBROS' 
               WHEN T9."DocNum" IS NOT NULL THEN 'COBROS ANTICIPOS PEDIDOS'
               WHEN T8."DocNum" IS NULL AND T9."DocNum" IS NULL THEN 'PAGO A CUENTA' END) AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", IFNULL(T9."DocNum",T12."TransId")) AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , ROUND((CASE WHEN (T8."DocNum" IS NULL AND T9."DocNum" IS NULL AND T12."TransId" IS NULL) THEN T0."TrsfrSum"
                ELSE (T7."SumApplied"*T0."TrsfrSum")/T0."DocTotal" END),2) AS "Importe"
        , T10."AcctName" AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."OACT" T10 ON T10."AcctCode" = T0."TrsfrAcct"
      LEFT JOIN "SBO_HVGA_PROD"."OJDT" T12 ON T12."TransId" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."TrsfrSum" > 0

      UNION ALL

      --T. Credito, Tarjeta de Regalo
      SELECT (CASE WHEN T10."CardName" = 'TARJETA REGALO' THEN 'TARJETA DE REGALO'
                   WHEN T10."CardName" LIKE 'RET%' THEN T10."CardName"
                  ELSE 
                      (CASE T10."U_HBT_tipo" WHEN 'DB' THEN 'TARJETA DEBITO' 
                                            WHEN 'CR' THEN 'TARJETA CRÉDITO' END)
                      || (CASE WHEN T10."U_HBT_tipo" = 'CR' AND T11."NumOfPmnts" > 1 THEN ' DIFERIDO' ELSE ' CORRIENTE' END) 
              END) AS "Tipo"
        , (CASE WHEN T8."DocNum" IS NOT NULL THEN 'COBROS' 
               WHEN T9."DocNum" IS NOT NULL THEN 'COBROS ANTICIPOS PEDIDOS'
               WHEN T8."DocNum" IS NULL AND T9."DocNum" IS NULL THEN 'PAGO A CUENTA' END) AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", IFNULL(T9."DocNum",T12."TransId")) AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , (CASE WHEN T7."SumApplied" IS NULL THEN T11."CreditSum"
            ELSE ROUND((CASE WHEN (T8."DocNum" IS NULL AND T9."DocNum" IS NULL AND T12."TransId" IS NULL) THEN T0."CreditSum"
                    ELSE (T7."SumApplied"*T11."CreditSum")/T0."DocTotal" END), 2) 
            END) AS "Importe"
        , '' AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      INNER JOIN "SBO_HVGA_PROD"."RCT3" T11 ON T11."DocNum" = T0."DocEntry" 
      INNER JOIN "SBO_HVGA_PROD"."OCRC" T10 ON T10."CreditCard" = T11."CreditCard"
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."OJDT" T12 ON T12."TransId" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."CreditSum" > 0

      UNION ALL

      --PAGOS A CUENTA (Cheques)
      SELECT  'CHEQUE' AS "Tipo"
        , 'PAGO A CUENTA' AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", T9."DocNum") AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , ROUND((T0."NoDocSum"*T1."CheckSum")/T0."DocTotal",2) AS "Importe"
        , T4."BankName" AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."RCT1" T1 ON T0."DocEntry" = T1."DocNum" 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."ODSC" T4 ON T1."BankCode" = T4."BankCode"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."CheckSum" > 0
        AND T7."SumApplied" > 0 AND T0."NoDocSum" > 0 

      UNION ALL

      --PAGOS A CUENTA (Efectivo)
      SELECT  'EFECTIVO' AS "Tipo"
        , 'PAGO A CUENTA' AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", T9."DocNum") AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , ROUND((T0."NoDocSum"*T0."CashSum")/T0."DocTotal",2) AS "Importe"
        , '' AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."CashSum" > 0
        AND T7."SumApplied" > 0 AND T0."NoDocSum" > 0

      UNION ALL

      --PAGOS A CUENTA (Transferencia)
      SELECT  'TRANSFERENCIA' AS "Tipo"
        , 'PAGO A CUENTA' AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", T9."DocNum") AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , ROUND((T0."NoDocSum"*T0."TrsfrSum")/T0."DocTotal",2) AS "Importe"
        , T10."AcctName" AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."OACT" T10 ON T10."AcctCode" = T0."TrsfrAcct"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."TrsfrSum" > 0
        AND T7."SumApplied" > 0 AND T0."NoDocSum" > 0

      UNION ALL

      --PAGOS A CUENTA (Tarjetas)
      SELECT (CASE WHEN T10."CardName" = 'TARJETA REGALO' THEN 'TARJETA DE REGALO'
                  ELSE 
                      (CASE T10."U_HBT_tipo" WHEN 'DB' THEN 'TARJETA DEBITO' 
                                            WHEN 'CR' THEN 'TARJETA CRÉDITO' END)
                      || (CASE WHEN T10."U_HBT_tipo" = 'CR' AND T11."NumOfPmnts" > 1 THEN ' DIFERIDO' ELSE ' CORRIENTE' END) 
              END) AS "Tipo"
        , 'PAGO A CUENTA' AS "GrupoCobro"
        , T3."SeriesName"
        , T3."BeginStr"
        , T0."DocNum" AS "NumPago"
        , IFNULL(T8."DocNum", T9."DocNum") AS "Fact"
        , CAST(T0."DocDate" AS DATE) AS "FechaPago"
        , T0."CardName"
        , ROUND((T0."NoDocSum"*T0."CreditSum")/T0."DocTotal",2) AS "Importe"
        , '' AS "Banco"
      FROM "SBO_HVGA_PROD"."ORCT" T0 
      LEFT JOIN "SBO_HVGA_PROD"."NNM1" T3 ON T0."Series" = T3."Series"
      LEFT JOIN "SBO_HVGA_PROD"."RCT2" T7 ON T7."DocNum" = T0."DocEntry" 
      INNER JOIN "SBO_HVGA_PROD"."RCT3" T11 ON T11."DocNum" = T0."DocEntry" 
      INNER JOIN "SBO_HVGA_PROD"."OCRC" T10 ON T10."CreditCard" = T11."CreditCard"
      LEFT JOIN "SBO_HVGA_PROD"."OINV" T8 ON T8."DocEntry" = T7."DocEntry"
      LEFT JOIN "SBO_HVGA_PROD"."ODPI" T9 ON T9."DocEntry" = T7."DocEntry"
      WHERE T0."Canceled" = 'N' AND T0."DocDate" = '${fechaSeleccionada}' AND T0."CreditSum" > 0
        AND T7."SumApplied" > 0 AND T0."NoDocSum" > 0

      ORDER BY "FechaPago", "NumPago"
    `;

    const rows = await new Promise((resolve, reject) => {
      conn.exec(query, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    // ✅ Filtrar por tiendas si se seleccionaron
    const filasFiltradas = tList.length > 0
      ? rows.filter(row => tList.includes(row.BeginStr))
      : rows;

    // Agrupar por tipo de pago
    const pagos = {};
    let totalGeneral = 0;
    filasFiltradas.forEach(row => {
      const tipo = row.Tipo;
      if (!pagos[tipo]) pagos[tipo] = [];
      pagos[tipo].push(row);
      totalGeneral += parseFloat(row.Importe || 0);
    });

    const totales = {};
    Object.keys(pagos).forEach(tipo => {
      totales[tipo] = pagos[tipo].reduce((sum, r) => sum + parseFloat(r.Importe || 0), 0);
    });

    const tiendasDisponibles = [
      { BeginStr: '001', SeriesName: 'Bosque' },
      { BeginStr: '002', SeriesName: 'Tumbaco' },
      { BeginStr: '003', SeriesName: 'Ibarra' },
      { BeginStr: '004', SeriesName: 'Plaza' },
      { BeginStr: '005', SeriesName: 'Cuenca' },
      { BeginStr: '006', SeriesName: 'Loja' }
    ];

    res.render('cierre_diario', {
      fechaSeleccionada,
      pagos,
      totales,
      totalGeneral,
      tiendasDisponibles,
      tList
    });
  } catch (error) {
    console.error('Error en /cierre-diario:', error);
    res.status(500).render('error', { message: error.message });
  } finally {
    if (conn) conn.disconnect();
  }
});

// ===================================================================
// 🔹 RUTA: /cierre-acumulado - ELIMINADA (como solicitado)
// ===================================================================
app.get('/cierre-acumulado', (req, res) => {
  res.redirect('/');
});

// === INICIAR SERVIDOR ===
app.listen(PORT, async () => {
  console.log(`✅ Servidor listo en http://localhost:${PORT}`);
  // Cargar caché desde archivo al iniciar
  await cargarCacheDesdeArchivo();
  console.log('👉 Abre en tu navegador:');
  console.log(`   • http://localhost:${PORT}/           → Menú principal`);
  console.log(`   • http://localhost:${PORT}/inventario  → Inventario`);
  console.log(`   • http://localhost:${PORT}/reporte-diario → Reporte diario`);
  console.log(`   • http://localhost:${PORT}/reporte-acumulado → Reporte acumulado`);
  console.log(`   • http://localhost:${PORT}/cierre-diario → Cierre diario`);
});

// Manejo de errores globales
process.on('unhandledRejection', (err) => {
  console.error('❌ Error no manejado:', err);
});
process.on('uncaughtException', (err) => {
  console.error('❌ Excepción no capturada:', err);
});