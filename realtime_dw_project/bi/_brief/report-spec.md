# 电商实时数仓运营看板扩展规格

本轮是棕地扩建：保留现有“实时经营总览”页面、1920×1080 画布与 Corporate Cool 浅色主题，只修复 PBIP 定义合法性；新增“订单生命周期”和“SCD2 版本审计”两个可用页面。受众同时覆盖运营分析人员和项目面试评审，重点展示订单 CDC 状态分支、退款回撤和商品维度历史版本。

数据与模型依据：

- 最终语义模型包含 9 张 Import 表、43 个度量值和 7 条单向关系；新增订单日期、共享渠道、订单每日指标与商品版本历史模型。
- 新页面数据来自 ClickHouse `ads_order_daily FINAL` 与 `dim_product_scd2 FINAL`，查询必须过滤 `is_deleted = 0`。
- 现有页面截图作为棕地基线保存在 `artifacts/powerbi_before/实时经营总览.png`。
- 交付边界为本地 PBIP/PBIR，不发布到 Fabric Service。

```yaml
Design Brief:
  generated_by: powerbi-report-design
  contract_version: 1
  mode: brownfield
  design_identity:
    tone: "Corporate Cool：冷灰画布、白色卡片、深青与青蓝为主色，橙色只表示退款/提醒，Segoe UI，信息密度中等"
    signature: "S12 Modular grid：所有新增页面使用统一 8px 对齐、16–24px 间距、白色 8px 圆角容器"
    current_tone: "Corporate Cool"
    current_signature: "冷灰画布上的白色圆角卡片，深青与橙色承担指标身份"
  archetype: "Executive + Drill"
  preserve_existing_pages:
    - "实时经营总览：保持视觉、筛选和主题不变，仅修复 definition.pbir 的 schema 元数据"
  color_map:
    - { measure: "订单每日指标[总订单数]", color: "#0891B2", tint: "#CFFAFE" }
    - { measure: "订单每日指标[支付订单数]", color: "#0F766E", tint: "#CCFBF1" }
    - { measure: "订单每日指标[取消订单数]", color: "#64748B", tint: "#E2E8F0" }
    - { measure: "订单每日指标[退款订单数]", color: "#F59E0B", tint: "#FEF3C7" }
    - { measure: "订单每日指标[支付金额]", color: "#0F766E", tint: "#CCFBF1" }
    - { measure: "订单每日指标[退款金额]", color: "#F59E0B", tint: "#FEF3C7" }
    - { measure: "订单每日指标[净支付金额]", color: "#0891B2", tint: "#CFFAFE" }
    - { measure: "商品版本历史[版本记录数]", color: "#7C3AED", tint: "#EDE9FE" }
  pages:
    - name: "支付与退款的渠道差异可追踪到每日状态"
      display_name: "订单生命周期"
      role: detail
      archetype: Analytical
      layout_variant: B
      variant_rationale: "仅需日期和渠道两个筛选器，采用 Inline-Slicers 释放全宽给趋势、渠道比较和明细矩阵。"
      page_background: "#F1F5F9"
      layout_summary: "标题与两个筛选器同带；五项 KPI 合并为一个多值 cardVisual；中部金额趋势与状态分支；底部渠道净支付和明细矩阵。"
      layout_contract:
        canvas: { width: 1920, height: 1080, margin: 32, gutter: 24, snap: 8 }
        grid:
          columns: 12
          rows: 12
          regions:
            header: [1, 1, 8, 2]
            filters: [8, 1, 13, 2]
            kpis: [1, 2, 13, 4]
            trend: [1, 4, 8, 8]
            outcomes: [8, 4, 13, 8]
            channel_amount: [1, 8, 7, 13]
            detail: [7, 8, 13, 13]
        placements:
          - { id: page_title, region: header, kind: textbox, text: "支付与退款的渠道差异可追踪到每日状态" }
          - { id: order_date_slicer, region: filters, kind: slicer, field_bindings: "订单日期[订单日期]", slicer_type: between, slot: 1, of: 2 }
          - { id: channel_slicer, region: filters, kind: slicer, field_bindings: "渠道[渠道]", slicer_type: dropdown, slot: 2, of: 2 }
          - id: lifecycle_kpis
            region: kpis
            kind: cardVisual
            purpose: "当前筛选范围的订单规模、支付结果、净收入和转化质量如何？"
            field_bindings: ["订单每日指标[总订单数]", "订单每日指标[支付订单数]", "订单每日指标[净支付金额]", "订单每日指标[支付率]", "订单每日指标[退款率]"]
            color_strategy: measure_match
          - id: paid_refund_trend
            region: trend
            kind: lineChart
            purpose: "支付金额与退款金额如何随日期变化？"
            field_bindings: { Category: "订单日期[订单日期]", Y: ["订单每日指标[支付金额]", "订单每日指标[退款金额]"] }
            sort_policy: category_asc
            color_strategy: measure_match
          - id: channel_outcomes
            region: outcomes
            kind: clusteredColumnChart
            purpose: "不同渠道的支付、取消和退款分支规模有何差异？"
            field_bindings: { Category: "渠道[渠道]", Y: ["订单每日指标[支付订单数]", "订单每日指标[取消订单数]", "订单每日指标[退款订单数]"] }
            sort_policy: value_desc
            color_strategy: measure_match
          - id: channel_net_paid
            region: channel_amount
            kind: clusteredColumnChart
            purpose: "哪个渠道贡献的退款后净支付金额最高？"
            field_bindings: { Category: "渠道[渠道]", Y: "订单每日指标[净支付金额]" }
            sort_policy: value_desc
            color_strategy: measure_match
          - id: channel_detail
            region: detail
            kind: pivotTable
            purpose: "查看每个渠道的订单、支付、退款、比率和客单价精确值。"
            field_bindings:
              Rows: "渠道[渠道]"
              Values: ["订单每日指标[总订单数]", "订单每日指标[支付订单数]", "订单每日指标[退款订单数]", "订单每日指标[净支付金额]", "订单每日指标[支付率]", "订单每日指标[退款率]", "订单每日指标[支付客单价]"]
            color_strategy: none
        space_audit:
          content_cell_count: 132
          placed_cell_count: 132
          empty_cell_pct: 0
          unplaced_regions: []
          largest_region: { name: detail, pct_of_content: 27 }
          balance_rationale: "KPI、两张中部分析图以及底部金额比较和精确明细完整覆盖内容区，没有空页脚或过大的单值卡片。"
    - name: "商品调价保留完整历史版本与生效区间"
      display_name: "SCD2 版本审计"
      role: detail
      archetype: Analytical
      layout_variant: A
      variant_rationale: "需要商品 ID 与当前标记筛选，并以历史版本排行配合宽表审计；左侧比较、右侧明细比纯 KPI 页面更符合审计任务。"
      page_background: "#F1F5F9"
      layout_summary: "标题与筛选器同带；四项审计 KPI；左侧版本记录排行，右侧完整版本区间表。"
      layout_contract:
        canvas: { width: 1920, height: 1080, margin: 32, gutter: 24, snap: 8 }
        grid:
          columns: 12
          rows: 12
          regions:
            header: [1, 1, 8, 2]
            filters: [8, 1, 13, 2]
            kpis: [1, 2, 13, 4]
            version_rank: [1, 4, 5, 13]
            version_detail: [5, 4, 13, 13]
        placements:
          - { id: page_title, region: header, kind: textbox, text: "商品调价保留完整历史版本与生效区间" }
          - { id: product_id_slicer, region: filters, kind: slicer, field_bindings: "商品版本历史[商品 ID]", slicer_type: dropdown, slot: 1, of: 2 }
          - { id: current_flag_slicer, region: filters, kind: slicer, field_bindings: "商品版本历史[是否当前]", slicer_type: dropdown, slot: 2, of: 2 }
          - id: scd2_kpis
            region: kpis
            kind: cardVisual
            purpose: "有多少商品、版本、当前记录和发生过调价的商品？"
            field_bindings: ["商品版本历史[商品数]", "商品版本历史[版本记录数]", "商品版本历史[当前版本数]", "商品版本历史[调价商品数]"]
            color_strategy: measure_match
          - id: version_rank
            region: version_rank
            kind: clusteredBarChart
            purpose: "哪些商品保留的历史版本最多？"
            field_bindings: { Category: "商品版本历史[商品名称]", Y: "商品版本历史[版本记录数]" }
            sort_policy: value_desc
            color_strategy: gradient
          - id: version_detail
            region: version_detail
            kind: tableEx
            purpose: "逐行核验每个商品版本的价格、生效区间和当前标记。"
            field_bindings: ["商品版本历史[商品 ID]", "商品版本历史[商品名称]", "商品版本历史[品类名称]", "商品版本历史[版本号]", "商品版本历史[价格]", "商品版本历史[生效时间]", "商品版本历史[失效时间]", "商品版本历史[是否当前]"]
            color_strategy: none
        space_audit:
          content_cell_count: 132
          placed_cell_count: 132
          empty_cell_pct: 0
          unplaced_regions: []
          largest_region: { name: version_detail, pct_of_content: 55 }
          balance_rationale: "审计任务以精确版本表为主，右侧宽表获得更大区域；左侧排行仍有 4×9 网格单元，足够识别多版本商品。"
  interaction_pattern:
    drill_targets: []
    cross_filter_rules: "保留 Power BI 默认交叉筛选；切片器过滤各自页面全部数据视觉。"
  accessibility:
    alt_text_strategy: "图表使用结构+发现模板，卡片使用指标+上下文模板；Tab 顺序与从左到右、从上到下的视觉顺序一致。"
    contrast_notes: "#0F172A/#FFFFFF 与 #475569/#FFFFFF 满足正文对比；支付、取消、退款同时有图例和文字标签，颜色不是唯一信号。"
  theme:
    base: "existing theme preserved"
    user_overrides: "不改现有主题文件、现有首页画布或已验证的视觉配色。"
```

验收标准：新增表和度量均有说明与格式；PBIR 校验无错误；Desktop 能重新打开并截图三页；新增页面没有空视觉、重叠或截断；同一指标跨视觉颜色保持一致。
