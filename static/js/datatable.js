document.addEventListener("DOMContentLoaded", function () {
    fetch('/data')
        .then(response => response.json())
        .then(result => {
            if (!result.data || !result.columns) {
                console.error("Error: Data or columns missing from the response");
                return;
            }

            // Set up the table header and filter row dynamically
            const tableHead = document.getElementById('table-head');
            const filterRow = document.getElementById('filter-row');

            result.columns.forEach((col, index) => {
                // Create header for each column
                const th = document.createElement('th');
                th.textContent = col.title;
                tableHead.appendChild(th);

                // Create header filter input dynamically
                const filterTh = document.createElement('th');
                const filterInput = document.createElement('input');
                filterInput.type = 'text';
                filterInput.placeholder = `Search ${col.title}`;
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
                columns: result.columns,
                paging: true,
                searching: true,
                info: false,
                scrollCollapse: false,
                scrollX: true,
                scrollY: '30vh',
                responsive: false,
                fixedColumns: true,
                columnDefs: [
                    { targets: 2, width: '500px' } // Set the width of the third column
                ],
                orderCellsTop: true, // Ensures that the header filters are above the table
                fixedHeader: true, // Keeps header fixed as you scroll
                pageLength: 25 // Adjust page length if needed
            });

            $(window).trigger('resize'); // Ensure the DataTable fits in its container
        })
        .catch(error => console.error('Error loading data:', error));
});
