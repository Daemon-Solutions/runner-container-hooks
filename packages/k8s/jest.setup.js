// Mock execCpToPod to prevent real network/file operations in tests
jest.mock('../src/k8s', () => {
	const actual = jest.requireActual('../src/k8s');
	return {
		...actual,
		execCpToPod: jest.fn().mockResolvedValue(undefined),
	};
});
// eslint-disable-next-line filenames/match-regex, no-undef
jest.setTimeout(500000)

// Mock @kubernetes/client-node CoreV1Api methods to prevent real cluster connections
jest.mock('@kubernetes/client-node', () => {
	const actual = jest.requireActual('@kubernetes/client-node');
	class MockCoreV1Api {
		createNamespacedPod = jest.fn().mockResolvedValue({ metadata: { name: 'mock-pod' }, status: { phase: 'Running' } });
		deleteNamespacedPod = jest.fn().mockResolvedValue({});
		listNamespacedPod = jest.fn().mockResolvedValue({ items: [] });
		listNamespacedSecret = jest.fn().mockResolvedValue({ items: [] });
		deleteNamespacedSecret = jest.fn().mockResolvedValue({});
		readNamespacedPod = jest.fn().mockResolvedValue({ metadata: { name: 'mock-pod' }, status: { phase: 'Running' } });
		patchNamespacedPod = jest.fn().mockResolvedValue({});
		// Add more mocked methods as needed for your tests
	}
	return {
		...actual,
		CoreV1Api: MockCoreV1Api,
		KubeConfig: actual.KubeConfig,
		// Export other classes as needed
	};
});
