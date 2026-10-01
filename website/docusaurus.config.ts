import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'ArcRun',
  tagline:
    'Task orchestration with DAG dependencies, concurrency control and webhook actions',
  favicon: 'img/icon.png',

  future: {
    v4: true,
  },

  // GitHub Pages: https://blumana-org.github.io/ArcRun/
  url: 'https://blumana-org.github.io',
  baseUrl: '/ArcRun/',
  organizationName: 'blumana-org',
  projectName: 'ArcRun',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    // The docs in ../docs are plain Markdown (not MDX): angle-bracket
    // placeholders like `<token>` must not be parsed as JSX.
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  presets: [
    [
      'classic',
      {
        docs: {
          // Serve the repository's docs/ folder directly - no copy step.
          path: '../docs',
          routeBasePath: '/docs',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/blumana-org/ArcRun/tree/master/',
          // Internal working documents (plans, audits, handoff notes) are not
          // part of the published documentation.
          exclude: [
            'audits/**',
            'plan/**',
            'handoff.md',
            'METRICS_PLAN.md',
            'perf-correctness-plan.md',
          ],
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/icon.png',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'ArcRun',
      logo: {
        alt: 'ArcRun',
        src: 'img/icon.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Documentation',
        },
        {
          href: 'https://hub.docker.com/r/plawn/arcrun',
          label: 'Docker Hub',
          position: 'right',
        },
        {
          href: 'https://github.com/blumana-org/ArcRun',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Concepts', to: '/docs/concepts'},
            {label: 'API Reference', to: '/docs/api'},
            {label: 'Configuration', to: '/docs/configuration'},
          ],
        },
        {
          title: 'Internals',
          items: [
            {label: 'Architecture', to: '/docs/architecture'},
            {label: 'Workers & Rules', to: '/docs/workers'},
            {label: 'Webhook Delivery', to: '/docs/webhooks'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'GitHub', href: 'https://github.com/blumana-org/ArcRun'},
            {label: 'Docker Hub', href: 'https://hub.docker.com/r/plawn/arcrun'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} ArcRun contributors.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'json', 'rust', 'sql', 'toml', 'yaml'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
