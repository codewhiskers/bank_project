// Fetch data from Flask backend and create a D3 chart
fetch('/data')
    .then(response => response.json())
    .then(data => {
        // Set up SVG canvas dimensions
        const width = 600, height = 400, margin = 40;
        const svg = d3.select("#chart")
                      .append("svg")
                      .attr("width", width)
                      .attr("height", height);

        // Define scales
        const xScale = d3.scaleLinear()
                         .domain(d3.extent(data, d => d.x))
                         .range([margin, width - margin]);
        const yScale = d3.scaleLinear()
                         .domain([0, d3.max(data, d => d.y)])
                         .range([height - margin, margin]);

        // Draw axes
        svg.append("g")
           .attr("transform", `translate(0, ${height - margin})`)
           .call(d3.axisBottom(xScale));
        svg.append("g")
           .attr("transform", `translate(${margin}, 0)`)
           .call(d3.axisLeft(yScale));

        // Plot data as circles
        svg.selectAll("circle")
           .data(data)
           .enter()
           .append("circle")
           .attr("cx", d => xScale(d.x))
           .attr("cy", d => yScale(d.y))
           .attr("r", 5)
           .attr("fill", "steelblue");
    })
    .catch(error => console.error("Error loading data:", error));
