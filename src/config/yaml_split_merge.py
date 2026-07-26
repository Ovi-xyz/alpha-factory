"""
yaml_split_merge.py — GMI Decision Document v5 §2.1 (Decision B Steps 2-3)

Shared deep-merge utility untuk sisi baca dari positional/structural join
Decision B: config/instruments_identity.yaml (sourcing/identity fields) +
config/instruments_taxonomy.yaml (routing/scoring fields + _meta blocks).

Dipakai oleh DUA konsumen — src/config/instrument_loader.py DAN
scripts/validate_instruments.py — supaya logika join tidak terduplikasi
dan berisiko divergen antar keduanya (prinsip yang sama dengan
_validate_commodity_taxonomy() single-helper di validate_instruments.py:
satu implementasi, bukan dua yang bisa diam-diam berbeda).

Kontrak join (Decision Document v5 §2.1):
  - SAMA tree path & SAMA index list = SATU instrumen/konsep. Bukan
    flat-list-plus-explicit-symbol-key join.
  - 'symbol' boleh muncul di KEDUA sisi pada setiap instrument dict — ini
    bukan pelanggaran disjointness, melainkan anchor yang disengaja:
    kalau kedua sisi tidak setuju pada index yang sama, itu adalah bukti
    solid file sudah tidak selaras (root cause paling mungkin: seseorang
    menyisip/menghapus/reorder instrumen di satu file tanpa mencerminkan
    perubahan yang sama di file lainnya). Merge RAISE keras di situasi
    itu — silent misattribution field ke instrumen yang salah adalah
    kelas bug yang jauh lebih berbahaya daripada crash yang jelas.
  - Field lain yang muncul di KEDUA sisi pada index yang sama adalah
    ValueError — split seharusnya field-disjoint sempurna kecuali anchor
    key di atas.
  - Dict node lain (bukan instrument-bearing list): union key, recurse.
  - '_meta' blocks: taxonomy-only secara desain (lihat ADR-027 di
    CHANGELOG.md) — hadir sebagai dict utuh di sisi taxonomy, absent di
    sisi identity. Ditangani otomatis oleh dict-merge generik di bawah,
    tidak butuh case khusus.
"""

from __future__ import annotations

# Field(s) yang SENGAJA boleh muncul di kedua file sebagai anchor/cross-check,
# bukan sebagai pelanggaran field-disjointness split.
SHARED_ANCHOR_KEYS: frozenset = frozenset({"symbol"})


def merge_split_trees(identity: dict, taxonomy: dict) -> dict:
    """
    Deep-merge instruments_identity.yaml + instruments_taxonomy.yaml
    menjadi satu dict, setara dengan bentuk instruments.yaml sebelum
    split (v1.5 dan sebelumnya). Non-destructive terhadap kedua input.

    Raise ValueError jika kedua file tidak selaras secara struktural
    (panjang list beda, urutan symbol beda, atau field overlap yang
    tidak terduga) — lebih baik gagal keras saat load daripada
    menghasilkan Instrument/validasi yang silently salah.
    """
    return _merge_node(identity, taxonomy, path="$")


def _merge_node(a, b, path: str):
    if a is None:
        return b
    if b is None:
        return a

    if isinstance(a, dict) and isinstance(b, dict):
        out: dict = {}
        for k in a.keys():
            out[k] = _merge_node(a[k], b.get(k), f"{path}.{k}")
        for k in b.keys():
            if k not in a:
                out[k] = b[k]
        return out

    if isinstance(a, list) and isinstance(b, list):
        return _merge_lists(a, b, path)

    # Scalar di kedua sisi pada path yang sama — hanya valid jika identik
    # (mis. top-level 'version'/'last_updated' kalau kebetulan disalin ke
    # kedua file dengan nilai yang sama). Nilai berbeda = data tidak sinkron.
    if a == b:
        return a
    raise ValueError(
        f"yaml_split_merge: conflicting scalar at {path}:"
        f" identity={a!r} vs taxonomy={b!r}"
    )


def _merge_lists(a: list, b: list, path: str) -> list:
    if not a:
        return list(b)
    if not b:
        return list(a)
    if len(a) != len(b):
        raise ValueError(
            f"yaml_split_merge: list length mismatch at {path} —"
            f" identity has {len(a)} item(s), taxonomy has {len(b)}."
            " Positional join requires equal length and matching order"
            " in both files."
        )

    merged = []
    for i, (ia, ib) in enumerate(zip(a, b)):
        item_path = f"{path}[{i}]"
        if isinstance(ia, dict) and isinstance(ib, dict):
            shared_keys = set(ia.keys()) & set(ib.keys())
            unexpected = shared_keys - SHARED_ANCHOR_KEYS
            if unexpected:
                raise ValueError(
                    f"yaml_split_merge: unexpected field overlap at"
                    f" {item_path}: {sorted(unexpected)} present in BOTH"
                    " identity and taxonomy — split is not field-disjoint."
                )
            for k in shared_keys & SHARED_ANCHOR_KEYS:
                if ia[k] != ib[k]:
                    raise ValueError(
                        f"yaml_split_merge: anchor key '{k}' mismatch at"
                        f" {item_path}: identity={ia[k]!r} vs"
                        f" taxonomy={ib[k]!r} — positional join is"
                        " misaligned (files edited out of sync)."
                    )
            merged.append({**ia, **ib})
        elif ia == ib:
            # Scalar list items (mis. 'central_banks: [ECB, BOE, ...]') —
            # hanya terjadi kalau kedua sisi punya list yang identik di
            # path yang sama, yang valid (tidak butuh merge field-level).
            merged.append(ia)
        else:
            raise ValueError(
                f"yaml_split_merge: list item mismatch at {item_path}:"
                f" {ia!r} vs {ib!r}"
            )
    return merged
