<AgGridReact
  // ...your existing props
  onGridReady={(params) => {
    // ensure the side panel is visible and open to Filters (or Columns)
    params.api.setSideBarVisible(true);
    params.api.openToolPanel("filters"); // or "columns"
  }}
  sideBar={{
    position: "right",
    hiddenByDefault: false,           // <-- show it on load
    defaultToolPanel: "filters",      // <-- which tab to show first ("filters" or "columns")
    toolPanels: [
      {
        id: "columns",
        labelDefault: "Columns",
        labelKey: "columns",
        iconKey: "columns",
        toolPanel: "agColumnsToolPanel",
      },
      {
        id: "filters",
        labelDefault: "Filters",
        labelKey: "filters",
        iconKey: "filter",
        toolPanel: "agFiltersToolPanel",
      },
    ],
  }}
/>
