document.addEventListener("DOMContentLoaded", function () {
    fetch('/data')
        .then(response => response.json())
        .then(result => {
            if (!result.data || !result.columns) {
                console.error("Error: Data or columns missing from the response");
                return;
            }

            // Mapping for custom display names for columns
            const columnTitleMap = {
                'entity_type': 'Entity Type',
                'rssd_id': 'RSSD ID',
                'entity_name': 'Entity Name',
                'city': 'City',
                'state': 'State',
            };

            // Set up the table header and filter row dynamically
            const tableHead = document.getElementById('table-head');
            const filterRow = document.getElementById('filter-row');

            result.columns.forEach((col, index) => {
                // Use custom title if available, otherwise use original column name
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

                // Attach event listener for filter input (attach immediately after creating the input)
                filterInput.addEventListener('keyup', function () {
                    const colIndex = filterInput.getAttribute('data-index');
                    const searchTerm = filterInput.value;

                    console.log(`Filtering column ${colIndex} with value: ${searchTerm}`);

                    // Use DataTables API to apply column filter
                    dataTable
                        .column(colIndex)
                        .search(searchTerm)
                        .draw();
                });
            });

            // Initialize DataTable
            const dataTable = $('#datatable').DataTable({
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
                columnDefs: [
                    { targets: 1, width: '165px' }, // Set the width of the first column
                    { targets: 2, width: '500px' } // Set the width of the third column
                ],
                orderCellsTop: true, // Ensures that the header filters are above the table
                fixedHeader: true, // Keeps header fixed as you scroll
                pageLength: 25 // Adjust page length if needed
            });
            $('#datatable tbody').on('click', 'tr', function () {
                $('#datatable tbody tr').removeClass('selected');
                $(this).addClass('selected'); // multiple selections
                console.log('Row selected:', dataTable.row(this).data());
                // $(this).toggleClass('selected'); // for one selection
            });

            $(window).trigger('resize'); // Ensure the DataTable fits in its container
        })
        .catch(error => console.error('Error loading data:', error));
});
