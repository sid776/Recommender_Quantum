<AgGridReact
  ref={gridRef}
  rowData={normRows}
  columnDefs={columnDefs}
  defaultColDef={{
    flex: 1,
    minWidth: 110,
    sortable: true,
    filter: true,
    resizable: true,
    enableValue: true,
    enableRowGroup: true,
    enablePivot: true,
    enableCharts: true,
    floatingFilter: showFloatingFilters,
  }}

  /* 👉 Right-hand tool panel */
  sideBar={{
    toolPanels: [
      { id: "columns", labelDefault: "Columns", iconKey: "columns", toolPanel: "agColumnsToolPanel" },
      { id: "filters",  labelDefault: "Filters",  iconKey: "filter",  toolPanel: "agFiltersToolPanel" },
    ],
    defaultToolPanel: "filters", // or "columns" or undefined
    position: "right",           // keep it on the right
    hiddenByDefault: false,      // show it initially (optional)
  }}

  autoGroupColumnDef={{
    headerName: "Group",
    minWidth: 260,
    pinned: "left",
  }}
  headerHeight={42}
  floatingFiltersHeight={36}
  loading={loading}
  animateRows
  enableRangeSelection
  suppressAggFuncInHeader
  onFirstDataRendered={onFirstDataRendered}
  suppressHorizontalScroll={false}
/>
