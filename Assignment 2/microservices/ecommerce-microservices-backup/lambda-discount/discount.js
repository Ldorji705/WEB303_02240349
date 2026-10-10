const http = require("http");

function handler(event) {
    try {
        const total = Number(event.total);
        const discountPercent = Number(event.discountPercent);

        // Validate input
        if (isNaN(total) || isNaN(discountPercent)) {
            return {
                success: false,
                error: "Total and discountPercent must be numbers"
            };
        }

        if (total < 0) {
            return {
                success: false,
                error: "Total cannot be negative"
            };
        }

        if (discountPercent < 0 || discountPercent > 100) {
            return {
                success: false,
                error: "Discount must be between 0 and 100"
            };
        }

        // Calculate discount
        const discount = total * (discountPercent / 100);
        const finalTotal = total - discount;

        return {
            success: true,
            originalTotal: total,
            discountPercent: discountPercent,
            discount: discount,
            finalTotal: finalTotal
        };

    } catch (error) {
        return {
            success: false,
            error: "Unable to calculate discount"
        };
    }
}

// HTTP server
const server = http.createServer((req, res) => {

    if (req.method !== "POST" || req.url !== "/discount") {
        res.writeHead(404, {
            "Content-Type": "application/json"
        });

        res.end(JSON.stringify({
            success: false,
            error: "Endpoint not found"
        }));

        return;
    }

    let body = "";

    req.on("data", chunk => {
        body += chunk;
    });

    req.on("end", () => {
        try {
            const event = JSON.parse(body);
            const result = handler(event);

            res.writeHead(result.success ? 200 : 400, {
                "Content-Type": "application/json"
            });

            res.end(JSON.stringify(result));

        } catch (error) {
            res.writeHead(400, {
                "Content-Type": "application/json"
            });

            res.end(JSON.stringify({
                success: false,
                error: "Invalid JSON request"
            }));
        }
    });
});

server.listen(3001, () => {
    console.log("Discount function running on http://localhost:3001");
    console.log("Endpoint: POST /discount");
});

module.exports = { handler };