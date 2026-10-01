import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';

const features: {title: string; description: string}[] = [
  {
    title: 'DAG dependencies',
    description:
      'Tasks depend on other tasks, with cascading propagation of failures and cancellations through the graph.',
  },
  {
    title: 'Concurrency & capacity rules',
    description:
      'Database-enforced limits on how many matching tasks run at once, or how much work they carry.',
  },
  {
    title: 'Webhook actions',
    description:
      'Start work through on_start webhooks, then deliver end and cancel notifications through a durable queue with retries.',
  },
  {
    title: 'Batch lifecycle',
    description:
      'Batch progress, cancellation, rule updates and durable completion notifications with retries.',
  },
  {
    title: 'Observability',
    description:
      'Prometheus metrics, OpenTelemetry tracing, health & readiness probes, built-in DAG visualization.',
  },
  {
    title: 'Hardened',
    description:
      'SSRF protection on webhook URLs, input validation, circuit breaker on the connection pool, optional bearer auth.',
  },
];

const quickStart = `docker pull plawn/arcrun:latest

docker run -e DATABASE_URL=postgres://user:pass@host/db \\
  -e HOST_URL=http://localhost:8085 \\
  -p 8085:8085 plawn/arcrun:latest`;

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const logo = useBaseUrl('/img/icon.png');
  return (
    <Layout title="Home" description={siteConfig.tagline}>
      <header className="hero hero--primary" style={{padding: '4rem 0'}}>
        <div className="container" style={{textAlign: 'center'}}>
          <img src={logo} alt="" width={96} height={96} />
          <Heading as="h1" className="hero__title">
            {siteConfig.title}
          </Heading>
          <p className="hero__subtitle">{siteConfig.tagline}</p>
          <div style={{display: 'flex', gap: '1rem', justifyContent: 'center'}}>
            <Link className="button button--secondary button--lg" to="/docs/getting-started">
              Get started
            </Link>
            <Link className="button button--outline button--secondary button--lg" to="/docs/api">
              API reference
            </Link>
          </div>
        </div>
      </header>
      <main className="container" style={{padding: '3rem 1rem'}}>
        <div className="row">
          {features.map((f) => (
            <div key={f.title} className="col col--4" style={{marginBottom: '2rem'}}>
              <Heading as="h3">{f.title}</Heading>
              <p>{f.description}</p>
            </div>
          ))}
        </div>
        <Heading as="h2">Quick start</Heading>
        <CodeBlock language="bash">{quickStart}</CodeBlock>
        <p>
          See <Link to="/docs/configuration">Configuration</Link> for every environment variable,
          and <Link to="/docs/concepts">Concepts</Link> for dependencies, rules and deduplication.
        </p>
      </main>
    </Layout>
  );
}
