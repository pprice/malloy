import {
  createSignal,
  createMemo,
  For,
  Component,
  mergeProps,
  Show,
  Switch,
  Match,
  JSXElement,
  JSX,
  onMount,
} from 'solid-js';
import {
  AtomicField,
  DataArrayOrRecord,
  DataRecord,
  Field,
} from '@malloydata/malloy';
import {getRangeSize, isFirstChild, isLastChild} from '../util';
import {getTableLayout} from './table-layout';
import {useResultContext} from '../result-context';
import {createTableStore, TableContext, useTableContext} from './table-context';
import tableCss from './table.css?raw';
import {applyRenderer} from '../apply-renderer';
import {isFieldHidden} from '../../tags_utils';
import {createStore, produce} from 'solid-js/store';
import {createVirtualizer, Virtualizer} from '@tanstack/solid-virtual';
import {useConfig} from '../render';
import {DimensionContextEntry} from '../types';
import {copyExplorePathQueryToClipboard} from '../result-store/result-store';

const IS_CHROMIUM = navigator.userAgent.toLowerCase().indexOf('chrome') >= 0;
// CSS Subgrid + Sticky Positioning only seems to work reliably in Chrome
const SUPPORTS_STICKY = IS_CHROMIUM;

const Cell = (props: {
  field: Field;
  value: JSXElement;
  hideStartGutter: boolean;
  hideEndGutter: boolean;
  isHeader?: boolean;
  tableGutterLeft?: boolean;
  tableGutterRight?: boolean;
  rawValue?: unknown;
  isChart?: boolean;
}) => {
  const style = () => {
    const layout = useTableContext()!.layout;
    const columnTag = props.field.tagParse().tag.tag('column');
    const width = layout.fieldLayout(props.field).width;
    const height = layout.fieldLayout(props.field).height;
    const style: JSX.CSSProperties = {};
    if (!props.isHeader) {
      if (width) {
        style.width = `${width}px`;
        style['min-width'] = `${width}px`;
        style['max-width'] = `${width}px;`;
      }
      if (height) {
        style.height = `${height}px`;
      }
      if (columnTag?.text('word_break') === 'break_all') {
        style['word-break'] = 'break-all';
      }
    }
    return style;
  };

  const config = useConfig();
  const handleClick = (evt: MouseEvent) => {
    if (config.onClick)
      config.onClick({
        field: props.field,
        displayValue: props.isHeader
          ? props.rawValue
          : typeof props.value !== 'function'
          ? props.value
          : null,
        value: typeof props.rawValue !== 'function' ? props.rawValue : null,
        fieldPath: props.field.fieldPath,
        isHeader: !!props.isHeader,
        event: evt,
        type: 'table-cell',
      });
  };

  return (
    <div
      class="cell-content"
      classList={{
        'header': props.isHeader,
        'hide-start-gutter': props.hideStartGutter,
        'hide-end-gutter': props.hideEndGutter,
        'table-gutter-left': !!props.tableGutterLeft,
        'table-gutter-right': !!props.tableGutterRight,
        'chart': !!props.isChart,
      }}
      style={style()}
      title={typeof props.value === 'string' ? props.value : ''}
      onClick={config.onClick ? handleClick : undefined}
    >
      {props.value}
    </div>
  );
};

const HeaderField = (props: {field: Field; isPinned?: boolean}) => {
  const {layout: tableLayout} = useTableContext()!;
  const metadata = useResultContext();
  const isFirst = isFirstChild(props.field);
  const isParentFirst = isFirstChild(props.field.parentExplore);
  const isParentNotAField = !props.field.parentExplore.isExploreField();
  const hideStartGutter = isFirst && (isParentFirst || isParentNotAField);

  const isLast = isLastChild(props.field);
  const isParentLast = isLastChild(props.field.parentExplore);
  const hideEndGutter = isLast && (isParentLast || isParentNotAField);

  const fieldLayout = tableLayout.fieldLayout(props.field);
  const columnRange = props.isPinned
    ? fieldLayout.absoluteColumnRange
    : fieldLayout.relativeColumnRange;

  const tableGutterLeft =
    (fieldLayout.depth > 0 && isFirstChild(props.field)) ||
    fieldLayout.metadata.renderAs === 'table';
  const tableGutterRight =
    (fieldLayout.depth > 0 && isLastChild(props.field)) ||
    (fieldLayout.depth === 0 && fieldLayout.metadata.renderAs === 'table');

  const customLabel = props.field.tagParse().tag.text('label');
  const value = customLabel ?? props.field.name.replace(/_/g, '_\u200b');

  return (
    <div
      class="column-cell th"
      data-pinned-header={
        props.isPinned ? metadata.getFieldKey(props.field) : undefined
      }
      classList={{
        'numeric': props.field.isAtomicField() && props.field.isNumber(),
        'pinned-header': props.isPinned,
      }}
      style={{
        'grid-column': `${columnRange[0] + 1} / span ${getRangeSize(
          columnRange
        )}`,
        'height': !props.isPinned
          ? `var(--malloy-render--table-header-height-${fieldLayout.depth})`
          : undefined,
      }}
    >
      <Cell
        field={props.field}
        // Add zero-width space so header name will wrap on _
        value={value}
        hideStartGutter={hideStartGutter}
        hideEndGutter={hideEndGutter}
        tableGutterLeft={tableGutterLeft}
        tableGutterRight={tableGutterRight}
        isHeader
        rawValue={props.field.name}
      />
    </div>
  );
};

const DRILL_RENDERER_IGNORE_LIST = ['chart'];
const TableField = (props: {
  field: Field;
  row: DataRecord;
  rowPath: number[];
  dimensionContext: DimensionContextEntry[];
}) => {
  let renderValue: JSXElement = '';
  let renderAs = '';
  ({renderValue, renderAs} = applyRenderer({
    field: props.field,
    dataColumn: props.row.cell(props.field),
    resultMetadata: useResultContext(),
    tag: props.field.tagParse().tag,
    customProps: {
      table: {
        rowLimit: 100, // Limit nested tables to 100 records
        currentRow: [...props.rowPath],
        // dimensionContext,
      },
    },
  }));
  const tableLayout = useTableContext()!.layout;
  const fieldLayout = tableLayout.fieldLayout(props.field);
  const columnRange = fieldLayout.relativeColumnRange;
  const tableCtx = useTableContext();
  const isHighlightedRow = () => {
    return (
      JSON.stringify(props.rowPath) ===
        JSON.stringify(tableCtx?.store.highlightedRow) &&
      JSON.stringify(tableCtx?.currentExplore) ===
        JSON.stringify(tableCtx?.store.highlightedExplore)
    );
  };
  const style: JSX.CSSProperties = {
    'grid-column': `${columnRange[0] + 1} / span ${getRangeSize(columnRange)}`,
    'height': 'fit-content',
  };

  if (renderAs === 'table') {
    style.display = 'grid';
    style.grid = 'auto / subgrid';
  }
  // TODO: review what should be sticky
  else if (SUPPORTS_STICKY && props.field.isAtomicField()) {
    style.position = 'sticky';
    style.top = `var(--malloy-render--table-header-cumulative-height-${fieldLayout.depth})`;
  }

  const tableGutterLeft = fieldLayout.depth > 0 && isFirstChild(props.field);
  const tableGutterRight = fieldLayout.depth > 0 && isLastChild(props.field);

  const handleMouseEnter = () => {
    if (tableCtx && !DRILL_RENDERER_IGNORE_LIST.includes(renderAs)) {
      // TODO: only update if changed; need to check change via stringify
      tableCtx.setStore(s => ({
        ...s,
        highlightedRow: props.rowPath,
      }));
    }
  };

  const handleMouseLeave = () => {
    tableCtx!.setStore(s => ({
      ...s,
      highlightedRow: null,
    }));
  };

  const config = useConfig();
  const isDrillingEnabled = config.tableConfig().enableDrill;
  const metadata = useResultContext();
  const handleClick = async evt => {
    evt.stopPropagation();
    if (isDrillingEnabled && !DRILL_RENDERER_IGNORE_LIST.includes(renderAs)) {
      copyExplorePathQueryToClipboard({
        metadata,
        field: props.field,
        dimensionContext: [
          ...tableCtx!.dimensionContext,
          ...props.dimensionContext,
        ],
        onDrill: config.onDrill,
      });
    }
  };

  return (
    <div
      class="column-cell td"
      classList={{
        numeric: props.field.isAtomicField() && props.field.isNumber(),
        highlight:
          isDrillingEnabled &&
          !DRILL_RENDERER_IGNORE_LIST.includes(renderAs) &&
          isHighlightedRow(),
      }}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      onClick={handleClick}
      style={style}
    >
      <Switch>
        {/* When table, skip cell wrapper */}
        <Match when={renderAs === 'table'}>{renderValue}</Match>
        <Match when>
          <Cell
            field={props.field}
            value={renderValue}
            hideStartGutter={isFirstChild(props.field)}
            hideEndGutter={isLastChild(props.field)}
            tableGutterLeft={tableGutterLeft}
            tableGutterRight={tableGutterRight}
            rawValue={props.row.cell(props.field).value}
            isChart={renderAs === 'chart'}
          />
        </Match>
      </Switch>
    </div>
  );
};

const MalloyTableRoot = (_props: {
  data: DataArrayOrRecord;
  rowLimit?: number;
  scrollEl?: HTMLElement;
  disableVirtualization?: boolean;
  shouldFillWidth?: boolean;
}) => {
  const props = mergeProps({rowLimit: Infinity}, _props);
  const tableCtx = useTableContext()!;
  const resultMetadata = useResultContext();

  let shouldFillWidth = false;
  if (tableCtx.root) {
    const sizeTag = props.data.field.tagParse().tag.tag('table')?.tag('size');
    if (sizeTag && sizeTag.text() === 'fill') shouldFillWidth = true;
    else if (typeof props.shouldFillWidth === 'boolean')
      shouldFillWidth = props.shouldFillWidth;
  }

  const pinnedFields = createMemo(() => {
    const fields = Object.entries(tableCtx.layout.fieldHeaderRangeMap)
      .sort((a, b) => {
        if (a[1].depth < b[1].depth) return -1;
        else if (a[1].depth > b[1].depth) return 1;
        else if (a[1].abs[0] < b[1].abs[0]) return -1;
        else if (a[1].abs[0] > b[1].abs[0]) return 1;
        else return 0;
      })
      .filter(([key, value]) => {
        const field = resultMetadata.fields[key].field;
        const parentFieldRenderer = field.parentExplore
          ? resultMetadata.field(field.parentExplore)?.renderAs
          : null;
        const isNotRoot = value.depth >= 0;
        const isPartOfTable = isNotRoot && parentFieldRenderer === 'table';
        return isPartOfTable;
      })
      .map(([key, value]) => ({
        fieldKey: key,
        field: resultMetadata.fields[key].field,
        ...value,
      }));
    return fields;
  });

  const maxPinnedHeaderDepth = createMemo(() => {
    return Math.max(...pinnedFields().map(f => f.depth));
  });

  const fieldsToSize = createMemo(() => {
    const fields = Object.entries(tableCtx.layout.fieldHeaderRangeMap).filter(
      ([key]) => !resultMetadata.fields[key].field.isExplore()
    );
    return fields;
  });

  const getContainerStyle: () => JSX.CSSProperties = () => {
    const columnRange = tableCtx.layout.fieldLayout(
      props.data.field
    ).relativeColumnRange;

    const style: JSX.CSSProperties = {
      '--table-row-span': getRangeSize(columnRange),
    };

    if (tableCtx.root) {
      let cumulativeDepth = 0;
      Object.entries(tableCtx.headerSizeStore[0])
        .sort(([a], [b]) => parseInt(a) - parseInt(b))
        .forEach(([depth, size]) => {
          style[`--malloy-render--table-header-height-${depth}`] = `${size}px`;
          cumulativeDepth += size;
          style[
            `--malloy-render--table-header-cumulative-height-${depth}`
          ] = `${cumulativeDepth}px`;
        });

      style['--total-header-size'] = tableCtx.layout.totalHeaderSize;
      const templateColumns = fieldsToSize()
        .map(([key]) => {
          const maybeSize = tableCtx.store.columnWidths[key];
          if (shouldFillWidth) return maybeSize ? maybeSize + 'px' : 'auto';
          return `minmax(${
            maybeSize ? maybeSize + 'px' : 'auto'
          }, max-content)`;
        })
        .join(' ');
      style['grid-template-columns'] = templateColumns;
    }
    return style;
  };

  // TODO: Get this from resultMetadata cache?
  const [rowsAreLimited, setRowsAreLimited] = createSignal(false);
  const data = createMemo(() => {
    const data: DataRecord[] = [];
    let i = 0;
    for (const row of props.data) {
      if (i >= props.rowLimit) {
        setRowsAreLimited(true);
        break;
      }
      data.push(row);
      i++;
    }
    return data;
  });

  const visibleFields = () =>
    props.data.field.allFields.filter(f => !isFieldHidden(f));

  const pinnedFieldsByDepth = () => {
    const fields = pinnedFields();
    return fields.reduce<(typeof fields)[]>((acc, curr) => {
      const list = acc[curr.depth] || (acc[curr.depth] = []);
      list.push(curr);
      return acc;
    }, []);
  };

  /*
    Detect pinned by checking if the body has scrolled content offscreen,
    but the pinned content is still fully visible.
  */
  let bodyDetector: HTMLDivElement | undefined;
  let pinnedDetector: HTMLDivElement | undefined;
  const [bodyOffscreen, setBodyOffscreen] = createSignal(false);
  const [pinnedOffscreen, setPinnedOffscreen] = createSignal(false);
  const pinned = () => bodyOffscreen() && !pinnedOffscreen();
  onMount(() => {
    if (bodyDetector && pinnedDetector) {
      const observer = new IntersectionObserver(
        ([e]) => {
          setBodyOffscreen(e.intersectionRatio < 1);
        },
        {threshold: [1]}
      );
      observer.observe(bodyDetector);

      const observer2 = new IntersectionObserver(
        ([e]) => {
          setPinnedOffscreen(e.intersectionRatio < 1);
        },
        {threshold: [1]}
      );
      observer2.observe(pinnedDetector);
    }
  });

  const headerMeasureEls: HTMLDivElement[] = [];
  onMount(() => {
    if (tableCtx.root) {
      const [headerSizes, setHeaderSizes] = tableCtx.headerSizeStore;
      const resizeObserver = new ResizeObserver(entries => {
        const changes = entries.reduce((acc, e) => {
          const depth = e.target.getAttribute('data-depth');
          const newHeight = e.contentRect.height;
          if (typeof depth === 'string' && headerSizes[depth] !== newHeight) {
            acc[depth] = newHeight;
          }
          return acc;
        }, {});
        if (Object.entries(changes).length > 0)
          setHeaderSizes(store => {
            return {
              ...store,
              ...changes,
            };
          });
      });

      headerMeasureEls.forEach(el => {
        resizeObserver.observe(el);
      });
    }
  });

  let scrollEl!: HTMLElement;
  if (props.scrollEl) scrollEl = props.scrollEl;
  let virtualizer: Virtualizer<HTMLElement, Element> | undefined;

  // Set arbitrarily large initial height to deal with weird measurement bug https://github.com/TanStack/virtual/issues/869
  // Could possibly try to pre-calculate a height estimate using metadata, but may be hard to do with potentially wrapping text so would have to add a buffer
  const [rowEstimate, setRowEstimate] = createSignal(1000);

  const shouldVirtualize = () => tableCtx.root && !props.disableVirtualization;

  if (shouldVirtualize()) {
    virtualizer = createVirtualizer({
      count: data().length,
      getScrollElement: () => scrollEl,
      estimateSize: () => rowEstimate(), // need better size estimate
    });
  }

  const items = virtualizer?.getVirtualItems();

  let pinnedHeaderRow!: HTMLDivElement;

  // Track scrolling state with 2s grace period
  // Used to only clear column width states on resize events that aren't due to data virtualization on scroll
  let isScrolling = false;
  const handleScroll = () => {
    isScrolling = true;
    setTimeout(() => {
      isScrolling = false;
    }, 2000);
  };

  function updateColumnWidths() {
    const pinnedHeaders = pinnedHeaderRow.querySelectorAll(
      '[data-pinned-header]'
    );
    const updates: [string, number][] = [];
    pinnedHeaders.forEach(node => {
      const key = node.getAttribute('data-pinned-header')!;
      const value = node.clientWidth;
      const currWidth = tableCtx.store.columnWidths[key];
      if (typeof currWidth === 'undefined' || value > currWidth)
        updates.push([key, value]);
    });
    if (updates.length > 0) {
      tableCtx.setStore(
        'columnWidths',
        produce((widths: Record<string, number>) => {
          updates.forEach(([key, value]) => (widths[key] = value));
        })
      );
    }
  }

  // Observe column width sizes and save them as they expand on scroll. Don't let them shrink as its jarring.
  onMount(() => {
    if (pinnedHeaderRow) {
      const resizeObserver = new ResizeObserver(() => {
        // select all nodes with data-pinned-header attribute
        if (isScrolling) {
          // Measure while scrolling
          updateColumnWidths();
        }
      });
      resizeObserver.observe(pinnedHeaderRow);
      // Initial measurement
      requestAnimationFrame(() => updateColumnWidths());
    }
  });

  // Observe table width resize
  // Clear width cache if table changes size due to something besides scroll position (fetching new data)
  // Meant to handle when the table resizes due to less available real estate, like a viewport change
  onMount(() => {
    if (tableCtx.root) {
      let priorWidth: number | null = null;
      const resizeObserver = new ResizeObserver(entries => {
        const [entry] = entries;
        // Not scrolling and skip the initial measurement, it's handled by header row observer
        if (!isScrolling && priorWidth !== null) {
          if (priorWidth !== entry.contentRect.width) {
            tableCtx.setStore(s => ({
              ...s,
              columnWidths: {},
            }));
          }
        }
        priorWidth = entry.contentRect.width;
      });
      resizeObserver.observe(scrollEl);
    }
  });

  const getPinnedHeaderRowStyle = () => ({
    'margin-bottom': `calc(-1 * (var(--malloy-render--table-header-cumulative-height-${maxPinnedHeaderDepth()}) - var(--malloy-render--table-header-height-0)))`,
  });

  const getRowPath = (rowIndex?: number) => {
    if (typeof rowIndex === 'number') return [...tableCtx.currentRow, rowIndex];
    return tableCtx.currentRow;
  };

  const handleTableMouseOver = (evt: MouseEvent) => {
    evt.stopPropagation();

    tableCtx.setStore(s => ({
      ...s,
      highlightedExplore: props.data.field.fieldPath,
    }));
  };

  const getRowDimensionContext = (row: DataRecord) => {
    const dimensionContext: DimensionContextEntry[] = [];

    const dimensions = row.field.allFields.filter(
      f => f.isAtomicField() && f.sourceWasDimension()
    ) as AtomicField[];
    dimensions.forEach(field => {
      dimensionContext.push({
        fieldDef: field.expression,
        value: row.cell(field).value as string | number | boolean | Date,
      });
    });
    return dimensionContext;
  };

  return (
    <div
      class="malloy-table"
      ref={el => {
        if (!props.scrollEl) scrollEl = el;
      }}
      classList={{
        'root': tableCtx.root,
        'full-width': shouldFillWidth,
        'pinned': pinned(),
      }}
      part={tableCtx.root ? 'table-container' : ''}
      style={getContainerStyle()}
      onScroll={handleScroll}
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      on:mouseover={handleTableMouseOver}
    >
      {/* pinned header */}
      <Show when={tableCtx.root}>
        <div
          ref={bodyDetector}
          style="position: sticky; left: 0px; height: 0px; visibility: hidden;"
        ></div>
        <div
          ref={pinnedDetector}
          style="position: sticky; top: 0px; left: 0px; height: 0px; visibility: hidden;"
        ></div>
        <div
          class="pinned-header-row"
          ref={pinnedHeaderRow}
          style={getPinnedHeaderRowStyle()}
        >
          <For each={pinnedFieldsByDepth()}>
            {(pinnedFields, idx) => (
              <div
                ref={el => (headerMeasureEls[idx()] = el)}
                data-depth={idx()}
                class="pinned-header-subrow"
              >
                <For each={pinnedFields}>
                  {pinnedField => (
                    <HeaderField field={pinnedField.field as Field} isPinned />
                  )}
                </For>
              </div>
            )}
          </For>
        </div>
      </Show>
      {/* virtualized table */}
      <Show when={shouldVirtualize()}>
        <div
          class="table-row"
          style={{
            height: virtualizer!.getTotalSize() + 'px',
            width: '100%',
            position: 'relative',
          }}
        >
          {/* second wrapper */}
          <div
            class="table-row"
            style={{
              'height': 'fit-content',
              'width': '100%',
              'padding-top': `${items![0]?.start ?? 0}px`,
            }}
          >
            <For each={items}>
              {virtualRow => (
                <div
                  class="table-row"
                  data-index={virtualRow.index}
                  data-row={JSON.stringify(getRowPath(virtualRow.index))}
                  ref={el =>
                    queueMicrotask(() => {
                      virtualizer!.measureElement(el);
                      // Use first row size as estimate for all rows
                      if (virtualRow.index === 0) {
                        setRowEstimate(el.clientHeight);
                      }
                    })
                  }
                >
                  <Show when={virtualRow.index >= 0}>
                    <For each={visibleFields()}>
                      {field => (
                        <TableField
                          field={field}
                          row={data()[virtualRow.index]}
                          rowPath={getRowPath(virtualRow.index)}
                          dimensionContext={getRowDimensionContext(
                            data()[virtualRow.index]
                          )}
                        />
                      )}
                    </For>
                  </Show>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      {/* non-virtualized table */}
      <Show when={!shouldVirtualize()}>
        {/* header */}
        <Show when={!tableCtx.root}>
          <div class="table-row" data-row={JSON.stringify(getRowPath())}>
            <For each={visibleFields()}>
              {field => <HeaderField field={field} />}
            </For>
          </div>
        </Show>
        {/* rows */}
        <For each={data()}>
          {(row, idx) => (
            <div class="table-row" data-row={JSON.stringify(getRowPath(idx()))}>
              <For each={visibleFields()}>
                {field => (
                  <TableField
                    field={field}
                    row={row}
                    rowPath={getRowPath(idx())}
                    dimensionContext={getRowDimensionContext(row)}
                  />
                )}
              </For>
            </div>
          )}
        </For>
        <Show when={rowsAreLimited()}>
          <div class="table-row limit-row table-gutter-left table-gutter-right">
            Limiting nested table to {props.rowLimit} records
          </div>
        </Show>
      </Show>
    </div>
  );
};

const MalloyTable: Component<{
  data: DataArrayOrRecord;
  rowLimit?: number;
  scrollEl?: HTMLElement;
  disableVirtualization?: boolean;
  shouldFillWidth?: boolean;
  currentRow?: number[];
  dimensionContext?: DimensionContextEntry[];
}> = props => {
  const metadata = useResultContext();
  const hasTableCtx = !!useTableContext();
  const tableCtx = createMemo<TableContext>(() => {
    if (hasTableCtx) {
      const parentCtx = useTableContext()!;
      const parentRecord = props.data.parentRecord;
      const dimensionContext: DimensionContextEntry[] = [];
      if (parentRecord) {
        const dimensions = parentRecord.field.allFields.filter(
          f => f.isAtomicField() && f.sourceWasDimension()
        ) as AtomicField[];
        dimensions.forEach(field => {
          dimensionContext.push({
            fieldDef: field.expression,
            value: parentRecord.cell(field).value as
              | string
              | number
              | boolean
              | Date,
          });
        });
      }
      return {
        ...parentCtx,
        root: false,
        currentRow: props.currentRow ?? parentCtx.currentRow,
        currentExplore: props.data.field.fieldPath,
        dimensionContext: [...parentCtx.dimensionContext, ...dimensionContext],
      };
    }

    const [store, setStore] = createTableStore();
    return {
      root: true,
      layout: getTableLayout(metadata, props.data.field),
      store,
      setStore,
      headerSizeStore: createStore({}),
      currentRow: [],
      currentExplore: props.data.field.fieldPath,
      dimensionContext: [],
    };
  });

  if (tableCtx().root) {
    const config = useConfig();
    config.addCSSToShadowRoot(tableCss);
  }

  const tableConfig = useConfig().tableConfig;

  const tableProps = () =>
    mergeProps(props, {
      disableVirtualization:
        typeof props.disableVirtualization === 'boolean'
          ? props.disableVirtualization
          : tableConfig().disableVirtualization,
      rowLimit:
        typeof props.rowLimit === 'number'
          ? props.rowLimit
          : tableConfig().rowLimit,
      shouldFillWidth:
        typeof props.shouldFillWidth === 'boolean'
          ? props.shouldFillWidth
          : tableConfig().shouldFillWidth,
    });

  return (
    <TableContext.Provider value={tableCtx()}>
      <MalloyTableRoot {...tableProps()} />
    </TableContext.Provider>
  );
};

export default MalloyTable;
