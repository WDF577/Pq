const fs = require("fs");
const path = require("path");

const reportDir = path.resolve(__dirname, "..", "电商实时数仓运营看板.Report");
const definitionDir = path.join(reportDir, "definition");
const pagesDir = path.join(definitionDir, "pages");
const pagesMetadataPath = path.join(pagesDir, "pages.json");
const definitionPbirPath = path.join(reportDir, "definition.pbir");

const VISUAL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.11.0/schema.json";
const TEXTBOX_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json";
const PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json";
const DEFINITION_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json";

const PAGE_LIFECYCLE = "ReportSection20260811dcdc000000000001";
const PAGE_SCD2 = "ReportSection20260811cdcd000000000002";

const palette = {
  ink: "#0F172A",
  muted: "#475569",
  subtle: "#64748B",
  canvas: "#F1F5F9",
  white: "#FFFFFF",
  card: "#F8FAFC",
  border: "#E2E8F0",
  cyan: "#0891B2",
  teal: "#0F766E",
  slate: "#64748B",
  orange: "#F59E0B",
  purple: "#7C3AED",
};

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function literal(value) {
  return { expr: { Literal: { Value: value } } };
}

function textValue(value) {
  return literal(`'${String(value).replaceAll("'", "''")}'`);
}

function boolValue(value) {
  return literal(value ? "true" : "false");
}

function decimalValue(value) {
  return literal(`${value}D`);
}

function integerValue(value) {
  return literal(`${value}L`);
}

function fill(color) {
  return { solid: { color: { expr: { Literal: { Value: `'${color}'` } } } } };
}

function columnField(entity, property) {
  return {
    Column: {
      Expression: { SourceRef: { Entity: entity } },
      Property: property,
    },
  };
}

function measureField(entity, property) {
  return {
    Measure: {
      Expression: { SourceRef: { Entity: entity } },
      Property: property,
    },
  };
}

function projection(fieldKind, entity, property) {
  return {
    field: fieldKind === "Measure" ? measureField(entity, property) : columnField(entity, property),
    queryRef: `${entity}.${property}`,
    nativeQueryRef: property,
  };
}

function position(x, y, width, height, order) {
  return { x, y, z: order, height, width, tabOrder: order };
}

function vco({ title, altText, padding = 8, background = palette.white, border = true }) {
  const result = {
    background: [
      {
        properties: {
          show: boolValue(true),
          color: fill(background),
          transparency: decimalValue(0),
        },
      },
    ],
    border: [
      {
        properties: {
          show: boolValue(border),
          color: fill(palette.border),
          width: decimalValue(1),
          radius: decimalValue(8),
        },
      },
    ],
    visualHeader: [{ properties: { show: boolValue(false) } }],
    padding: [
      {
        properties: {
          top: decimalValue(padding),
          bottom: decimalValue(padding),
          left: decimalValue(padding),
          right: decimalValue(padding),
        },
      },
    ],
    general: [{ properties: { altText: textValue(altText) } }],
  };

  if (title) {
    result.title = [
      {
        properties: {
          show: boolValue(true),
          text: textValue(title),
          fontFamily: textValue("Segoe UI Semibold"),
          fontSize: decimalValue(14),
          fontColor: fill(palette.ink),
          alignment: textValue("left"),
          titleWrap: boolValue(true),
        },
      },
    ];
  }

  return result;
}

function makePage(name, displayName) {
  return {
    $schema: PAGE_SCHEMA,
    name,
    displayName,
    displayOption: "FitToPage",
    height: 1080,
    width: 1920,
    objects: {
      background: [
        {
          properties: {
            color: fill(palette.canvas),
            transparency: decimalValue(0),
          },
        },
      ],
      outspace: [
        {
          properties: {
            color: fill(palette.border),
            transparency: decimalValue(0),
          },
        },
      ],
    },
  };
}

function makeTextbox(name, pos, title, subtitle) {
  return {
    $schema: TEXTBOX_SCHEMA,
    name,
    position: pos,
    visual: {
      visualType: "textbox",
      objects: {
        general: [
          {
            properties: {
              paragraphs: [
                {
                  textRuns: [
                    {
                      value: title,
                      textStyle: {
                        fontFamily: "Segoe UI Semibold",
                        fontSize: "24px",
                        color: palette.ink,
                      },
                    },
                  ],
                  horizontalTextAlignment: "left",
                },
                {
                  textRuns: [
                    {
                      value: subtitle,
                      textStyle: {
                        fontFamily: "Segoe UI",
                        fontSize: "11px",
                        color: palette.subtle,
                      },
                    },
                  ],
                  horizontalTextAlignment: "left",
                },
              ],
            },
          },
        ],
      },
      visualContainerObjects: vco({
        altText: `${title}。${subtitle}`,
        padding: 0,
        background: palette.canvas,
        border: false,
      }),
    },
  };
}

function makeSlicer(name, pos, entity, property, mode, headerText, altText) {
  return {
    $schema: VISUAL_SCHEMA,
    name,
    position: pos,
    visual: {
      visualType: "slicer",
      query: {
        queryState: {
          Values: { projections: [projection("Column", entity, property)] },
        },
      },
      objects: {
        data: [{ properties: { mode: textValue(mode) } }],
        header: [
          {
            properties: {
              show: boolValue(true),
              text: textValue(headerText),
              fontFamily: textValue("Segoe UI Semibold"),
              textSize: decimalValue(11),
              fontColor: fill(palette.ink),
              background: fill(palette.white),
              showRestatement: boolValue(false),
            },
          },
        ],
        selection: [
          {
            properties: {
              selectAllCheckboxEnabled: boolValue(false),
              singleSelect: boolValue(false),
              strictSingleSelect: boolValue(false),
            },
          },
        ],
      },
      visualContainerObjects: vco({ altText, padding: 8 }),
    },
  };
}

function makeKpiCard(name, pos, entity, measure, altText, accentColor) {
  return {
    $schema: TEXTBOX_SCHEMA,
    name,
    position: pos,
    visual: {
      visualType: "card",
      query: {
        queryState: {
          Values: { projections: [projection("Measure", entity, measure)] },
        },
      },
      objects: {
        labels: [
          {
            properties: {
              fontFamily: textValue("Segoe UI Semibold"),
              fontSize: decimalValue(26),
              bold: boolValue(true),
              color: fill(accentColor),
            },
          },
        ],
        categoryLabels: [
          {
            properties: {
              show: boolValue(true),
              fontFamily: textValue("Segoe UI"),
              fontSize: decimalValue(10),
              color: fill(palette.muted),
            },
          },
        ],
        wordWrap: [
          {
            properties: { show: boolValue(true) },
          },
        ],
      },
      visualContainerObjects: vco({ altText, padding: 8 }),
    },
  };
}

function chartObjects({
  colors,
  singleColor,
  line = false,
  showLegend = false,
  showLabels = false,
  horizontal = false,
}) {
  const objects = {
    dataPoint: singleColor
      ? [{ properties: { defaultColor: fill(singleColor) } }]
      : Object.entries(colors).map(([metadata, color]) => ({
          properties: { fill: fill(color), ...(line ? {} : { borderShow: boolValue(false) }) },
          selector: { metadata },
        })),
    labels: [
      {
        properties: {
          show: boolValue(showLabels),
          fontSize: decimalValue(10),
          color: fill(palette.muted),
          labelDisplayUnits: textValue("-1"),
          labelPosition: textValue(horizontal ? "OutsideEnd" : "OutsideEnd"),
        },
      },
    ],
    categoryAxis: [
      {
        properties: {
          show: boolValue(true),
          fontFamily: textValue("Segoe UI"),
          fontSize: decimalValue(10),
          labelColor: fill(palette.muted),
          showAxisTitle: boolValue(false),
          gridlineShow: boolValue(false),
        },
      },
    ],
    valueAxis: [
      {
        properties: {
          show: boolValue(true),
          start: decimalValue(0),
          fontFamily: textValue("Segoe UI"),
          fontSize: decimalValue(10),
          labelColor: fill(palette.muted),
          showAxisTitle: boolValue(false),
          gridlineShow: boolValue(true),
          gridlineColor: fill(palette.border),
          gridlineTransparency: decimalValue(35),
        },
      },
    ],
    legend: [
      {
        properties: {
          show: boolValue(showLegend),
          position: textValue("TopCenter"),
          showTitle: boolValue(false),
          fontFamily: textValue("Segoe UI"),
          fontSize: decimalValue(10),
          labelColor: fill(palette.muted),
        },
      },
    ],
  };

  if (line) {
    objects.lineStyles = [
      {
        properties: {
          strokeShow: boolValue(true),
          strokeWidth: decimalValue(3),
          lineChartType: textValue("smooth"),
          interpolationSmooth: textValue("monotoneX"),
          showMarker: boolValue(false),
          areaShow: boolValue(false),
        },
      },
    ];
  } else {
    objects.layout = [
      {
        properties: {
          clusteredGapSize: decimalValue(16),
          seriesOrderSorted: boolValue(false),
        },
      },
    ];
  }

  return objects;
}

function makeChart({
  name,
  pos,
  visualType,
  category,
  measures,
  title,
  altText,
  colors = {},
  singleColor,
  sort,
  filterConfig,
  showLegend = false,
  showLabels = false,
}) {
  const query = {
    queryState: {
      Category: { projections: [projection("Column", category.entity, category.property)] },
      Y: {
        projections: measures.map((item) => projection("Measure", item.entity, item.property)),
      },
    },
  };

  if (sort) {
    query.sortDefinition = {
      sort: [
        {
          field:
            sort.kind === "Column"
              ? columnField(sort.entity, sort.property)
              : measureField(sort.entity, sort.property),
          direction: sort.direction,
        },
      ],
      isDefaultSort: false,
    };
  }

  return {
    $schema: VISUAL_SCHEMA,
    name,
    position: pos,
    visual: {
      visualType,
      query,
      objects: chartObjects({
        colors,
        singleColor,
        line: visualType === "lineChart",
        showLegend,
        showLabels,
        horizontal: visualType === "clusteredBarChart",
      }),
      visualContainerObjects: vco({ title, altText, padding: 8 }),
      drillFilterOtherVisuals: true,
    },
    ...(filterConfig ? { filterConfig } : {}),
  };
}

function greaterThanMeasureFilter(name, entity, property, threshold) {
  const source = "m";
  return {
    filters: [
      {
        name,
        field: measureField(entity, property),
        type: "Advanced",
        filter: {
          Version: 2,
          From: [{ Name: source, Entity: entity, Type: 0 }],
          Where: [
            {
              Condition: {
                Comparison: {
                  ComparisonKind: 1,
                  Left: {
                    Measure: {
                      Expression: { SourceRef: { Source: source } },
                      Property: property,
                    },
                  },
                  Right: { Literal: { Value: `${threshold}L` } },
                },
              },
            },
          ],
        },
        howCreated: "User",
      },
    ],
  };
}

function tableObjects({ matrix = false }) {
  const result = {
    columnHeaders: [
      {
        properties: {
          columnAdjustment: textValue("growToFit"),
          autoSizeColumnWidth: boolValue(true),
          wordWrap: boolValue(true),
          fontFamily: textValue("Segoe UI Semibold"),
          fontSize: decimalValue(10),
          bold: boolValue(true),
          fontColor: fill(palette.ink),
          backColor: fill(palette.border),
          alignment: textValue("Center"),
        },
      },
    ],
    values: [
      {
        properties: {
          fontFamily: textValue("Segoe UI"),
          fontSize: decimalValue(10),
          fontColorPrimary: fill(palette.ink),
          fontColorSecondary: fill(palette.ink),
          backColorPrimary: fill(palette.white),
          backColorSecondary: fill(palette.card),
          wordWrap: boolValue(false),
        },
      },
    ],
    grid: [
      {
        properties: {
          gridVertical: boolValue(false),
          gridHorizontal: boolValue(true),
          gridHorizontalColor: fill(palette.border),
          gridHorizontalWeight: decimalValue(1),
          rowPadding: decimalValue(7),
        },
      },
    ],
  };

  if (matrix) {
    result.rowHeaders = [
      {
        properties: {
          fontFamily: textValue("Segoe UI Semibold"),
          fontSize: decimalValue(10),
          fontColor: fill(palette.ink),
          backColor: fill(palette.white),
          stepped: boolValue(false),
          repeatRowHeaders: boolValue(true),
          wordWrap: boolValue(false),
        },
      },
    ];
  }

  return result;
}

function makePivot(name, pos, row, values, title, altText) {
  return {
    $schema: VISUAL_SCHEMA,
    name,
    position: pos,
    visual: {
      visualType: "pivotTable",
      query: {
        queryState: {
          Rows: { projections: [projection("Column", row.entity, row.property)] },
          Values: {
            projections: values.map((item) => projection("Measure", item.entity, item.property)),
          },
        },
      },
      objects: tableObjects({ matrix: true }),
      visualContainerObjects: {
        ...vco({ title, altText, padding: 8 }),
        stylePreset: [{ properties: { name: textValue("None") } }],
      },
      drillFilterOtherVisuals: true,
    },
  };
}

function makeTable(name, pos, entity, columns, title, altText) {
  return {
    $schema: VISUAL_SCHEMA,
    name,
    position: pos,
    visual: {
      visualType: "tableEx",
      query: {
        queryState: {
          Values: { projections: columns.map((column) => projection("Column", entity, column)) },
        },
        sortDefinition: {
          sort: [
            { field: columnField(entity, "商品 ID"), direction: "Ascending" },
            { field: columnField(entity, "版本号"), direction: "Descending" },
          ],
          isDefaultSort: false,
        },
      },
      objects: tableObjects({ matrix: false }),
      visualContainerObjects: {
        ...vco({ title, altText, padding: 8 }),
        stylePreset: [{ properties: { name: textValue("None") } }],
      },
      drillFilterOtherVisuals: true,
    },
  };
}

function writeVisual(pageName, visual) {
  if (!/^[a-f0-9]{20}$/.test(visual.name)) {
    throw new Error(`Visual id must be 20 lowercase hex characters: ${visual.name}`);
  }
  writeJson(path.join(pagesDir, pageName, "visuals", visual.name, "visual.json"), visual);
}

function buildLifecyclePage() {
  const page = makePage(PAGE_LIFECYCLE, "订单生命周期");
  writeJson(path.join(pagesDir, PAGE_LIFECYCLE, "page.json"), page);

  const visuals = [
    makeTextbox(
      "c2010000000000000001",
      position(32, 24, 1032, 80, 1000),
      "支付与退款的渠道差异可追踪到每日状态",
      "按日期追踪资金回流，按渠道定位支付、取消与退款结构",
    ),
    makeSlicer(
      "c2020000000000000002",
      position(1088, 24, 376, 80, 2000),
      "订单日期",
      "订单日期",
      "Between",
      "订单日期范围",
      "订单日期范围筛选器，可选择开始日期和结束日期。",
    ),
    makeSlicer(
      "c2030000000000000003",
      position(1488, 24, 400, 80, 3000),
      "渠道",
      "渠道",
      "Dropdown",
      "渠道",
      "渠道下拉筛选器，用于筛选本页全部订单指标。",
    ),
    makeKpiCard(
      "c2040000000000000004",
      position(32, 128, 352, 144, 4000),
      "订单每日指标",
      "总订单数",
      "当前筛选范围内的总订单数。",
      palette.teal,
    ),
    makeKpiCard(
      "c2090000000000000009",
      position(400, 128, 352, 144, 4100),
      "订单每日指标",
      "支付订单数",
      "当前筛选范围内的支付订单数。",
      palette.cyan,
    ),
    makeKpiCard(
      "c20a000000000000000a",
      position(768, 128, 352, 144, 4200),
      "订单每日指标",
      "支付率",
      "支付订单数占总订单数的比例。",
      palette.teal,
    ),
    makeKpiCard(
      "c20b000000000000000b",
      position(1136, 128, 352, 144, 4300),
      "订单每日指标",
      "退款率",
      "退款订单数占支付订单数的比例。",
      palette.orange,
    ),
    makeKpiCard(
      "c20c000000000000000c",
      position(1504, 128, 384, 144, 4400),
      "订单每日指标",
      "净支付金额",
      "支付金额扣除退款金额后的净额。",
      palette.purple,
    ),
    makeChart({
      name: "c2050000000000000005",
      pos: position(32, 296, 1104, 344, 5000),
      visualType: "lineChart",
      category: { entity: "订单日期", property: "订单日期" },
      measures: [
        { entity: "订单每日指标", property: "支付金额" },
        { entity: "订单每日指标", property: "退款金额" },
      ],
      title: "每日支付与退款金额",
      altText: "折线图，按订单日期比较支付金额与退款金额，识别资金回流和退款波动。",
      colors: {
        "订单每日指标.支付金额": palette.teal,
        "订单每日指标.退款金额": palette.orange,
      },
      sort: { kind: "Column", entity: "订单日期", property: "订单日期", direction: "Ascending" },
      showLegend: true,
    }),
    makeChart({
      name: "c2060000000000000006",
      pos: position(1160, 296, 728, 344, 6000),
      visualType: "clusteredColumnChart",
      category: { entity: "渠道", property: "渠道" },
      measures: [
        { entity: "订单每日指标", property: "支付订单数" },
        { entity: "订单每日指标", property: "取消订单数" },
        { entity: "订单每日指标", property: "退款订单数" },
      ],
      title: "渠道订单状态分布",
      altText: "簇状柱形图，按渠道比较支付、取消和退款订单数。",
      colors: {
        "订单每日指标.支付订单数": palette.teal,
        "订单每日指标.取消订单数": palette.slate,
        "订单每日指标.退款订单数": palette.orange,
      },
      sort: { kind: "Measure", entity: "订单每日指标", property: "支付订单数", direction: "Descending" },
      showLegend: true,
    }),
    makeChart({
      name: "c2070000000000000007",
      pos: position(32, 664, 912, 376, 7000),
      visualType: "clusteredColumnChart",
      category: { entity: "渠道", property: "渠道" },
      measures: [{ entity: "订单每日指标", property: "净支付金额" }],
      title: "渠道净支付金额",
      altText: "柱形图，按渠道比较退款后的净支付金额并按金额降序排列。",
      singleColor: palette.cyan,
      sort: { kind: "Measure", entity: "订单每日指标", property: "净支付金额", direction: "Descending" },
      showLabels: true,
    }),
    makePivot(
      "c2080000000000000008",
      position(968, 664, 920, 376, 8000),
      { entity: "渠道", property: "渠道" },
      [
        { entity: "订单每日指标", property: "总订单数" },
        { entity: "订单每日指标", property: "支付订单数" },
        { entity: "订单每日指标", property: "退款订单数" },
        { entity: "订单每日指标", property: "净支付金额" },
        { entity: "订单每日指标", property: "支付率" },
        { entity: "订单每日指标", property: "退款率" },
        { entity: "订单每日指标", property: "支付客单价" },
      ],
      "渠道订单与资金明细",
      "透视表，逐渠道展示总订单、支付订单、退款订单、净支付金额、支付率、退款率和支付客单价。",
    ),
  ];

  visuals.forEach((visual) => writeVisual(PAGE_LIFECYCLE, visual));
}

function buildScd2Page() {
  const page = makePage(PAGE_SCD2, "SCD2 版本审计");
  writeJson(path.join(pagesDir, PAGE_SCD2, "page.json"), page);

  const visuals = [
    makeTextbox(
      "c3010000000000000001",
      position(32, 24, 1032, 80, 1000),
      "商品调价保留完整历史版本与生效区间",
      "核验版本数量、价格变化、生效边界与当前记录唯一性",
    ),
    makeSlicer(
      "c3020000000000000002",
      position(1088, 24, 376, 80, 2000),
      "商品版本历史",
      "商品 ID",
      "Dropdown",
      "商品 ID",
      "商品 ID 下拉筛选器，用于定位单个或多个商品的版本历史。",
    ),
    makeSlicer(
      "c3030000000000000003",
      position(1488, 24, 400, 80, 3000),
      "商品版本历史",
      "是否当前",
      "Dropdown",
      "是否当前版本",
      "是否当前版本下拉筛选器，使用 1 表示当前版本，0 表示历史版本。",
    ),
    makeKpiCard(
      "c3040000000000000004",
      position(32, 128, 448, 144, 4000),
      "商品版本历史",
      "商品数",
      "当前筛选范围内的去重商品数。",
      palette.purple,
    ),
    makeKpiCard(
      "c3070000000000000007",
      position(496, 128, 448, 144, 4100),
      "商品版本历史",
      "版本记录数",
      "当前筛选范围内的 SCD2 历史版本记录数。",
      palette.cyan,
    ),
    makeKpiCard(
      "c3080000000000000008",
      position(960, 128, 448, 144, 4200),
      "商品版本历史",
      "当前版本数",
      "当前筛选范围内标记为有效的版本数。",
      palette.teal,
    ),
    makeKpiCard(
      "c3090000000000000009",
      position(1424, 128, 464, 144, 4300),
      "商品版本历史",
      "调价商品数",
      "拥有两个及以上历史版本的商品数。",
      palette.orange,
    ),
    makeChart({
      name: "c3050000000000000005",
      pos: position(32, 296, 640, 744, 5000),
      visualType: "clusteredBarChart",
      category: { entity: "商品版本历史", property: "商品 ID" },
      measures: [{ entity: "商品版本历史", property: "版本记录数" }],
      title: "商品历史版本数量",
      altText: "条形图，按商品 ID 比较历史版本记录数量并按版本记录数降序排列。",
      singleColor: palette.purple,
      sort: { kind: "Measure", entity: "商品版本历史", property: "版本记录数", direction: "Descending" },
      filterConfig: greaterThanMeasureFilter(
        "c305f11e000000000001",
        "商品版本历史",
        "版本记录数",
        1,
      ),
      showLabels: true,
    }),
    makeTable(
      "c3060000000000000006",
      position(696, 296, 1192, 744, 6000),
      "商品版本历史",
      ["商品 ID", "版本号", "商品名称", "品类名称", "价格", "生效时间", "失效时间", "是否当前"],
      "商品 SCD2 版本明细",
      "表格，逐行展示商品 ID、版本号、商品名称、品类名称、价格、生效时间、失效时间和当前版本标记。",
    ),
  ];

  visuals.forEach((visual) => writeVisual(PAGE_SCD2, visual));
}

function updateDefinition() {
  const current = readJson(definitionPbirPath);
  const next = { $schema: DEFINITION_SCHEMA, ...current };
  writeJson(definitionPbirPath, next);
}

function updatePageOrder() {
  const metadata = readJson(pagesMetadataPath);
  const preserved = metadata.pageOrder.filter((name) => name !== PAGE_LIFECYCLE && name !== PAGE_SCD2);
  metadata.pageOrder = [...preserved, PAGE_LIFECYCLE, PAGE_SCD2];
  writeJson(pagesMetadataPath, metadata);
}

function assertStableIds() {
  for (const pageName of [PAGE_LIFECYCLE, PAGE_SCD2]) {
    if (!/^ReportSection[a-f0-9]{24}$/.test(pageName)) {
      throw new Error(`Page id must be ReportSection plus 24 lowercase hex characters: ${pageName}`);
    }
  }
}

assertStableIds();
updateDefinition();
buildLifecyclePage();
buildScd2Page();
updatePageOrder();

console.log(`Generated pages ${PAGE_LIFECYCLE} and ${PAGE_SCD2}`);
