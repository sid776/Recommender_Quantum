<Box
  className="ag-theme-alpine"
  style={{ height: "400px", width: "100%" }}
>
  <AgGridReact
    rowData={[{ test: "hello" }, { test: "world" }]}
    columnDefs={[{ headerName: "Test", field: "test" }]}
  />
</Box>
