# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt, numpy as np, pandas as pd

def bar_chart(df, x, y, title):
    fig, ax = plt.subplots(); ax.bar(df[x].astype(str), df[y])
    ax.set_title(title); ax.set_xlabel(x); ax.set_ylabel(y); ax.tick_params(axis='x', labelrotation=75); fig.tight_layout(); return fig
def line_chart(df, x, y, title):
    fig, ax = plt.subplots(); ax.plot(df[x], df[y], marker="o")
    ax.set_title(title); ax.set_xlabel(x); ax.set_ylabel(y); ax.grid(True, linestyle=":"); fig.tight_layout(); return fig
def hist_log_sizes(series, bins=50, title="Histograma de tamaños (log10 bytes)"):
    import numpy as np
    s = pd.to_numeric(series, errors="coerce").dropna(); s=s[s>0]
    if s.empty: return None
    fig, ax = plt.subplots(); ax.hist(np.log10(s), bins=bins)
    ax.set_title(title); ax.set_xlabel("log10(Bytes)"); ax.set_ylabel("Frecuencia"); fig.tight_layout(); return fig
def treemap_sliced(values, labels, title="Treemap"):
    vals = np.array(values, dtype=float); 
    if vals.sum()<=0: return None
    labels=list(labels); fig, ax = plt.subplots(); ax.set_title(title)
    x=y=0.0; w=h=1.0; total=vals.sum(); horizontal=True
    order=np.argsort(-vals); vals=vals[order]; labels=[labels[i] for i in order]
    for v,lab in zip(vals, labels):
        frac=v/total
        if horizontal:
            ww=frac*w; rect=plt.Rectangle((x,y), ww,h, fill=True, alpha=0.6); ax.add_patch(rect); ax.text(x+ww/2,y+h/2, str(lab), ha="center", va="center", fontsize=8); x+=ww
        else:
            hh=frac*h; rect=plt.Rectangle((x,y), w,hh, fill=True, alpha=0.6); ax.add_patch(rect); ax.text(x+w/2,y+hh/2, str(lab), ha="center", va="center", fontsize=8); y+=hh
        horizontal=not horizontal
    ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off"); fig.tight_layout(); return fig

def heatmap_pivot(pivot_df, title="Heatmap"):
    fig, ax = plt.subplots()
    im = ax.imshow(pivot_df.values, aspect="auto")
    ax.set_title(title); ax.set_xlabel(pivot_df.columns.name or "Col"); ax.set_ylabel(pivot_df.index.name or "Idx")
    ax.set_xticks(range(pivot_df.shape[1])); ax.set_xticklabels(list(pivot_df.columns), rotation=75, ha="right")
    ax.set_yticks(range(pivot_df.shape[0])); ax.set_yticklabels(list(pivot_df.index))
    fig.tight_layout(); return fig


def smart_time_series(df, x: str, y: str, title: str):
    import matplotlib.pyplot as plt, matplotlib.ticker as mticker
    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(df[x], df[y], marker="o")
    ax.set_title(title); ax.set_xlabel(x); ax.set_ylabel(y)
    ax.grid(True, linestyle=":", linewidth=0.7)
    ax.margins(x=0.02)
    # reducir etiquetas del eje X
    if len(df) > 16:
        step = max(1, len(df)//16)
        for i, label in enumerate(ax.get_xticklabels()):
            if i % step != 0: label.set_visible(False)
    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_ha("right")
    fig.tight_layout()
    return fig

def bar_top(df, x: str, y: str, title: str, top=20, horizontal=False):
    import matplotlib.pyplot as plt
    d = df[[x,y]].copy().dropna()
    d = d.sort_values(y, ascending=False).head(top)
    if horizontal:
        fig, ax = plt.subplots(figsize=(10,5))
        ax.barh(d[x].astype(str), d[y])
        ax.invert_yaxis()
        for i, v in enumerate(d[y].values):
            ax.text(v, i, f" {int(v)}", va="center")
        ax.set_xlabel(y); ax.set_ylabel(x)
    else:
        fig, ax = plt.subplots(figsize=(10,5))
        ax.bar(d[x].astype(str), d[y])
        for i, v in enumerate(d[y].values):
            ax.text(i, v, f"{int(v)}", ha="center", va="bottom", rotation=0)
        for lab in ax.get_xticklabels():
            lab.set_rotation(45); lab.set_ha("right")
        ax.set_ylabel(y); ax.set_xlabel(x)
    ax.set_title(title); ax.grid(axis="y", linestyle=":", linewidth=0.7)
    fig.tight_layout()
    return fig
