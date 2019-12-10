const { CLI, CREATE_DATABASE } = require('./cliConstants');

const getGlueDatabaseCreateStatement = (containerData) => {
	const dbParameters = {
		DatabaseInput: {
			Name: containerData.name,
			Description: containerData.description,
		}
	}

	const cliStatement = `${CLI} ${CREATE_DATABASE} '${JSON.stringify(dbParameters, null, 2)}'`;
	return cliStatement;
}

module.exports = {
	getGlueDatabaseCreateStatement
};