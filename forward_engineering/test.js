const { generateContainerScript } = require("./api");
const fs = require('fs');

const data = JSON.parse(fs.readFileSync('forward_engineering/data.json'));
const logger = {
    log: console.log,
    clear: () => {},
    progress: () => {},
}

generateContainerScript(data, logger, (sth, script) => {
    fs.writeFileSync('./result.json', null, 4);
})