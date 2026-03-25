// eslint-disable-next-line filenames/match-regex, no-undef
jest.setTimeout(500000)

// Mock @kubernetes/client-node CoreV1Api methods to prevent real cluster connections
jest.mock('@kubernetes/client-node', () => {
	const actual = jest.requireActual('@kubernetes/client-node');
	class MockCoreV1Api {
		createNamespacedPod = jest.fn().mockResolvedValue({ body: {} });
		deleteNamespacedPod = jest.fn().mockResolvedValue({ body: {} });
		listNamespacedPod = jest.fn().mockResolvedValue({ body: { items: [] } });
		listNamespacedSecret = jest.fn().mockResolvedValue({ body: { items: [] } });
		deleteNamespacedSecret = jest.fn().mockResolvedValue({ body: {} });
		readNamespacedPod = jest.fn().mockResolvedValue({ body: {} });
		patchNamespacedPod = jest.fn().mockResolvedValue({ body: {} });
		// Add more mocked methods as needed for your tests
	}
	return {
		...actual,
		CoreV1Api: MockCoreV1Api,
		KubeConfig: actual.KubeConfig,
		// Export other classes as needed
	};
});
