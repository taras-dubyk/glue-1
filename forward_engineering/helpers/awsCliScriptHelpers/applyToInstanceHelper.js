const { CLI, CREATE_DATABASE, CREATE_TABLE } = require('./cliConstants');

const dbRegex = new RegExp(`${CLI} ${CREATE_DATABASE} '(.*})'`,'i');
const tableRegex = new RegExp(`${CLI} ${CREATE_TABLE} '(.*})'`,'i');

const getApiStatements = (script) => {
	const cliStatements = script.split('\n\n');
	return cliStatements.reduce((acc, statement) => {
		const oneLineStatement = statement.replace(/\n/g, '');
		if (dbRegex.test(oneLineStatement)) {
			const parsedInput = getStatementInput(dbRegex, oneLineStatement);
			acc.db = [...acc.db, parsedInput];
		} else if (tableRegex.test(oneLineStatement)) {
			const parsedInput = getStatementInput(tableRegex, oneLineStatement);
			acc.table = [...acc.table, parsedInput];
		}
		return acc;
	}, { db: [], table: [] });
}

const getStatementInput = (regExp, statement) => {
	const jsonInput = regExp.exec(statement)[1];
	return JSON.parse(jsonInput);
}

module.exports = {
	getApiStatements
};