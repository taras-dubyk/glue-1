const { getTypeByProperty } = require('../columnHelper');

const getGlueTableColumns = (properties = {}) => {
	return Object.entries(properties)
		.filter(([key, value]) => !value.compositePartitionKey)
		.map(([key, value]) => mapColumn(key, value));
};

const getGluePartitionKeyTableColumns = (properties = {}) => {
	return Object.entries(properties)
		.filter(([key, value]) => value.compositePartitionKey)
		.map(([key, value]) => mapColumn(key, value));
};

const getGlueTableClusteringKeyColumns = (properties = {}) => {
	return Object.entries(properties)
		.filter(([key, value]) => value.compositeClusteringKey)
		.map(([key, value]) => key);
};

const getGlueTableSortingColumns = (sortingItems = [], properties = {}) => {
	return sortingItems.map(item => {
		const property = Object.entries(properties).find(([key, value]) => value.id === item.id);
		const propertyName = property && property[0];
		return {
			Column: propertyName,
			SortOrder: item.type === 'ascending' ? 1 : 0
		};
	});
};

const mapColumn = (name, data) => {
	return {
		Name: name,
		Type: getTypeByProperty(data),
		Comment: data.comments
	};
}

module.exports = {
	getGlueTableColumns,
	getGluePartitionKeyTableColumns,
	getGlueTableClusteringKeyColumns,
	getGlueTableSortingColumns
};
