'use strict'

const { buildStatement, getName, getTab, replaceSpaceWithUnderscore } = require('./generalHelper');

const getCreateStatement = ({
	name, comment, location, dbProperties, isActivated
}) => buildStatement(`CREATE DATABASE IF NOT EXISTS ${name}`, isActivated)
	(comment, `COMMENT "${comment}"`)
	(location, `LOCATION "${location}"`)
	(dbProperties, `WITH DBPROPERTIES (${dbProperties})`)
	(true, ';')
	();

const getDatabaseStatement = (containerData) => {
	const tab = getTab(0, containerData);
	
	return getCreateStatement({
		name: replaceSpaceWithUnderscore(getName(tab)),
		comment: tab.comments,
		isActivated: tab.isActivated
	});
};

module.exports = {
	getDatabaseStatement
};
