import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// Explicit ordering of the repository's docs/ folder. Doc ids are the file
// names (without extension); labels come from each file's H1.
const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Guide',
      collapsed: false,
      items: ['openapi_description', 'concepts', 'api', 'configuration', 'metrics'],
    },
    {
      type: 'category',
      label: 'Internals',
      collapsed: false,
      items: ['architecture', 'workers', 'webhooks'],
    },
  ],
};

export default sidebars;
