import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'ParqueDB',
    },
    links: [
      {
        text: 'GitHub',
        url: 'https://github.com/dot-do/parquedb',
      },
    ],
  };
}
