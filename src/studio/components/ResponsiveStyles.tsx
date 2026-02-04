/**
 * ResponsiveStyles Component
 *
 * Injects responsive CSS styles for the Studio dashboard.
 * This component is used to provide consistent responsive behavior
 * across all dashboard components.
 */

'use client'

const responsiveCSS = `
@media (max-width: 768px) {
  .database-dashboard {
    padding: 1rem !important;
  }

  .dashboard-toolbar {
    flex-direction: column;
    gap: 0.75rem !important;
  }

  .toolbar-search {
    flex: 1 1 100% !important;
  }

  .toolbar-actions {
    width: 100%;
    justify-content: space-between;
  }

  .database-grid {
    grid-template-columns: 1fr !important;
  }

  .batch-controls {
    width: 100%;
    justify-content: space-between;
  }
}

@media (max-width: 480px) {
  .dashboard-header h1 {
    font-size: 1.5rem !important;
  }

  .btn-create {
    width: 100%;
    justify-content: center;
  }

  .dashboard-footer {
    flex-direction: column;
    gap: 0.5rem !important;
  }
}
`

/**
 * Responsive CSS styles for the Studio dashboard
 */
export function ResponsiveStyles() {
  return (
    <style dangerouslySetInnerHTML={{ __html: responsiveCSS }} />
  )
}

export default ResponsiveStyles
