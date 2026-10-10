const { handler } = require("./discount");

const result = handler({
    total: -100,
    discountPercent: 10
});

console.log(result);