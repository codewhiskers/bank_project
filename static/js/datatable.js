const DataTableManager = (function () {
    const instances = {};

    function initializeDataTable(dataUrl, tableId, tableHeadId, filterRowId, columnTitleMap, callback) {
        fetch(dataUrl)
            .then(response => response.json())
            .then(result => {
                if (!result.data || !result.columns) {
                    console.error("Error: Data or columns missing from the response");
                    return;
                }

                // Set up the table header and filter row dynamically
                const tableHead = document.getElementById(tableHeadId);
                const filterRow = document.getElementById(filterRowId);

                // Clear any existing headers and filter rows to avoid duplicates
                tableHead.innerHTML = '';
                filterRow.innerHTML = '';

                result.columns.forEach((col, index) => {
                    const displayTitle = columnTitleMap[col.data] || col.data;

                    // Create header for each column
                    const th = document.createElement('th');
                    th.textContent = displayTitle;
                    tableHead.appendChild(th);

                    // Create header filter input dynamically
                    const filterTh = document.createElement('th');
                    const filterInput = document.createElement('input');
                    filterInput.type = 'text';
                    filterInput.placeholder = `Search ${displayTitle}`;
                    filterInput.style.width = '100%';
                    filterInput.setAttribute('data-index', index);
                    filterTh.appendChild(filterInput);
                    filterRow.appendChild(filterTh);

                    // Attach event listener for filter input
                    filterInput.addEventListener('keyup', function () {
                        const colIndex = filterInput.getAttribute('data-index');
                        const searchTerm = filterInput.value;

                        // Use DataTables API to apply column filter
                        instances[tableId]
                            .column(colIndex)
                            .search(searchTerm)
                            .draw();
                    });
                });

                // Initialize DataTable
                const dataTable = $(`#${tableId}`).DataTable({
                    data: result.data,
                    columns: result.columns.map(col => ({
                        data: col.data // Only bind the data key, not the title
                    })),
                    paging: true,
                    searching: true,
                    info: false,
                    scrollCollapse: false,
                    scrollX: true,
                    scrollY: '35vh',
                    responsive: false,
                    fixedColumns: true,
                    orderCellsTop: true,
                    fixedHeader: true,
                    pageLength: 25
                });

                // Store the DataTable instance for later reference
                instances[tableId] = dataTable;

                // Call the callback if provided
                if (callback) {
                    callback(dataTable);
                }

                $(`#${tableId} tbody`).on('click', 'tr', function () {
                    $(`#${tableId} tbody tr`).removeClass('selected');
                    $(this).addClass('selected');
                    console.log('Row selected:', instances[tableId].row(this).data());
                });

                $(window).trigger('resize');
            })
            .catch(error => console.error('Error loading data:', error));
    }

    function getInstance(tableId) {
        return instances[tableId];
    }

    return {
        initializeDataTable,
        getInstance
    };
})();