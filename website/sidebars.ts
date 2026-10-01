import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// Pages are served directly from the repository's docs/ directory.
const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Guides',
      collapsed: false,
      items: ['getting-started', 'concepts', 'webhooks'],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: ['api', 'configuration', 'metrics', 'openapi_description'],
    },
    {
      type: 'category',
      label: 'Implementation',
      collapsed: false,
      items: ['architecture', 'workers'],
    },
  ],
};

export default sidebars;
